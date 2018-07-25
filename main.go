package main

import (
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/billziss-gh/cgofuse/fuse"
	"github.com/vmware/go-nfs-client/nfs"
	nfsrpc "github.com/vmware/go-nfs-client/nfs/rpc"
)

const (
	fileCachedHandlerName           = "RPCHandler.FileCachedEvent"
	fileRemovedFromCacheHandlerName = "RPCHandler.FileRemovedFromCacheEvent"
	getFileDataHandlerName          = "RPCHandler.GetFileData"
	cacheDir                        = ".fusecache"
)

var (
	nfshost   = flag.String("nfs-server", "localhost", "The hostname/IP for the NFS Server.")
	target    = flag.String("nfs-target", "/C/temp", "The target/path on the NFS Server.")
	mntpoint  = flag.String("mount-point", "", "Where to mount the file system.")
	thisip    = flag.String("thisip", "", "The IP address of this server for cache requests.")
	remoteips = flag.String("remoteips", "", "Comma separate list of IPs for remote servers.")
	rpcPort   = flag.String("remoteport", "5555", "Port to use for connection with remote servers.")
	memLimit  = flag.Int("memlimitmb", 0, "Optionally limit how much memory can be used. Recommended, otherwise process will keep caching until something blows up.")

	nfsfs       *Nfsfs
	nfstarget   *nfs.Target
	cachelock   sync.Mutex
	cachedNodes []CachedNode
	cachePath   string

	remoteServers     []string
	remoteCachedFiles map[string]string
)

// Nfsfs is the main file system structure
type Nfsfs struct {
	fuse.FileSystemBase
	lock    sync.Mutex
	ino     uint64
	root    *Node
	openmap map[string]*Node
}

// Node represents a file or folder
type Node struct {
	stat     fuse.Stat_t
	xattr    map[string][]byte
	children map[string]*Node
	data     []byte
	lock     sync.Mutex
	opencnt  int
	cache    FileCache
}

// FileCache represents in memory cached byte ranges
type FileCache struct {
	byteRanges []ByteRange
	cachedNode CachedNode
	lock       sync.Mutex
}

// CachedNode represents when a node was cached. Used for cache-eviction
type CachedNode struct {
	path         string
	node         *Node
	timeCached   time.Time
	lastAccessed time.Time
}

// ByteRange for a chunk of cached data
type ByteRange struct {
	low  int64
	high int64
}

// CacheUpdateRequest to update remote server caches
type CacheUpdateRequest struct {
	Fromip   string
	Filepath string
	Fh       uint64
}

// CacheUpdateResponse in response to remote server cache updates
type CacheUpdateResponse struct {
	IsCached bool
}

// CachedDataRequest to request cached data from a remote server
type CachedDataRequest struct {
	Filepath  string
	Fh        uint64
	Offset    int64
	Endoffset int64
	Filedata  []byte
}

// CachedDataResponse in response to a request for cached data
type CachedDataResponse struct {
	Filedata  []byte
	NumbBytes int
}

// RPCHandler handler type for our RPC calls
type RPCHandler struct{}

// function definition to aid testing
type rpcCallFunc func(method string, args interface{}, reply interface{}) error

func main() {
	flag.Parse()
	log.SetOutput(os.Stdout)

	remoteCachedFiles = make(map[string]string)

	if len(*remoteips) > 0 {
		remoteServers = strings.Split(*remoteips, ",")
	}

	log.Println("NFS host:", *nfshost)
	log.Println("NFS target:", *target)
	log.Println("Mount point:", *mntpoint)
	log.Println("Remote servers:", len(remoteServers), remoteServers)
	log.Println("Remote port:", *rpcPort)
	log.Println("Memory limit:", *memLimit)

	if len(remoteServers) > 0 {
		go setupRPCListener(*rpcPort)
	}

	cachePath = createLocalCacheDir()

	err := connectNFS()
	if err != nil {
		os.Exit(1)
	}
	defer nfstarget.Close()
	log.Println("Connected to NFS", *nfshost, *target)

	// start go routine to monitor for changes to files that are cache
	go evictModifiedFilesFromCacheLoop()

	nfsfs = newfs()
	host := fuse.NewFileSystemHost(nfsfs)
	host.Mount(*mntpoint, []string{})
	defer host.Unmount()
}

func connectNFS() error {
	mount, err := nfs.DialMount(*nfshost)
	if err != nil {
		log.Println("NFS: unable to dial MOUNT service:", err.Error())
		return err
	}
	defer mount.Close()

	//TODO: Check if needed
	auth := nfsrpc.NewAuthUnix("", 1000, 1000)

	nfstarget, err = mount.Mount(*target, auth.Auth())
	if err != nil {
		log.Println("NFS: unable to mount volume:", err.Error())
	}
	return err
}

func setupRPCListener(port string) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Error setting up RPC listener. %v", err)
		return
	}
	defer listener.Close()

	err = rpc.Register(new(RPCHandler))
	if err != nil {
		log.Fatalf("Error registering RPC handler. %v", err)
		return
	}
	rpc.Accept(listener)
}

func newfs() *Nfsfs {
	fs := Nfsfs{}
	defer fs.synchronize()()
	fs.ino++
	fs.root = newNode(0, fs.ino, fuse.S_IFDIR|00777, 0, 0)
	fs.openmap = map[string]*Node{}
	return &fs
}

// Open opens the specified node
func (fs *Nfsfs) Open(path string, flags int) (errc int, fh uint64) {
	log.Println("Open() path: ", path)

	defer fs.synchronize()()

	errc, fh = fs.openNode(path, false)
	if errc != 0 {
		log.Fatalln("Open() - Error: ", errc)
	}
	return
}

// Getattr gets the attributes for a specified node
func (fs *Nfsfs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	log.Println("Getattr() path: ", path)

	defer fs.synchronize()()

	node := fs.getNode(path, fh)
	if nil == node {
		log.Fatalln("Getattr() Error: nil node", path)
		*stat = newNode(0, fh, uint32(fuse.S_IFREG|0444), 0, 0).stat
		return 0 //-fuse.ENOENT
	}

	*stat = node.stat
	return 0
}

// Getxattr gets the extended file attributes
func (fs *Nfsfs) Getxattr(path string, name string) (errc int, xattr []byte) {

	defer fs.synchronize()()

	_, _, node := fs.lookupNode(path, nil)

	if node == nil {
		log.Fatalln("Getxattr() Error: nil node")
		return -fuse.ENOENT, nil
	}

	xattr, ok := node.xattr[name]
	if !ok {
		log.Fatalln("Getxattr() Error: not 'ok'")
		return -fuse.ENOATTR, nil
	}

	return 0, xattr
}

// Truncate truncates the specified file
func (fs *Nfsfs) Truncate(path string, size int64, fh uint64) (errc int) {
	log.Println("Truncate() path: ", path)

	defer fs.synchronize()()

	node := fs.getNode(path, fh)
	if nil == node {
		log.Fatalln("Getxattr() - getNode(): node nil")
		return -fuse.ENOENT
	}

	node.data = resize(node.data, size, true)
	node.stat.Size = size
	tmsp := fuse.Now()
	node.stat.Ctim = tmsp
	node.stat.Mtim = tmsp
	return 0
}

// Read reads in the specified file
func (fs *Nfsfs) Read(path string, buff []byte, offset int64, fh uint64) (numb int) {

	defer fs.synchronize()()

	node := fs.getNode(path, fh)

	if node == nil {
		log.Println("Read() Error: nil node")
		return -fuse.ENOENT
	}

	offset, endoffset := calcOffsets(path, node, offset, buff)

	// If the file is larger than the specified memory limit then
	// go straight to NFS without calling any caching methods
	// TODO: should still cache to local file system. This is difficult
	// as we only write local file once we've cached the entire thing in memory
	if *memLimit != 0 && bToMb(node.stat.Size) > *memLimit {
		numb = readFileFromNFS(path, node, buff, offset, endoffset)
		if offset == 0 || endoffset == node.stat.Size {
			log.Println("Read() - read from NFS: \t", offset, endoffset, len(buff), path)
		}
		if numb > 0 {
			return numb
		}
	}

	// 1. See if we have this byte range cached locally in memory
	numb, newCacheItemRequired := fetchLocalCacheData(path, node, offset, endoffset, buff)
	if numb > 0 {
		return numb
	}

	// 2.  Check if the file is cached on disk
	numb = fetchLocalFSCacheData(path, node, offset, endoffset, buff)
	if numb > 0 {
		return numb
	}

	// 3. See if we have this file cached with one of our remotes
	numb = tryRemoteCache(path, fh, node, offset, endoffset, buff)

	if numb == 0 {
		// 4. Otherwise, get from NFS
		numb = readFileFromNFS(path, node, buff, offset, endoffset)
		if offset == 0 || endoffset == node.stat.Size {
			log.Println("Read() - read from NFS: \t", offset, endoffset, len(buff), path)
		}
	}

	updateLocalCache(newCacheItemRequired, path, node, buff, offset, endoffset)

	node.stat.Atim = fuse.Now()
	return numb
}

func calcOffsets(path string, node *Node, offset int64, buff []byte) (int64, int64) {
	endoffset := offset + int64(len(buff))
	if endoffset > node.stat.Size {
		endoffset = node.stat.Size
	}
	if endoffset < offset {
		return 0, 0
	}
	if offset == 0 {
		log.Println("Read() - start:\t\t", offset, endoffset, endoffset-offset, path)
	}
	if endoffset == node.stat.Size {
		log.Println("Read() - end:  \t\t", offset, endoffset, endoffset-offset, path)
	}
	return offset, endoffset
}

// Release releases the specified file handle
func (fs *Nfsfs) Release(path string, fh uint64) (errc int) {
	// log.Println("Release() path: ", path, fh)

	defer fs.synchronize()()
	return fs.closeNode(path)
}

// Opendir opens the specified directory/node
func (fs *Nfsfs) Opendir(path string) (errc int, fh uint64) {
	// log.Println("Opendir() path: ", path)

	defer fs.synchronize()()
	return fs.openNode(path, true)
}

// Readdir populates the specified path's files and folders
func (fs *Nfsfs) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {
	// log.Println("Readdir() ", path)

	defer fs.synchronize()()

	return fs.populateDir(path, fill)
}

func (fs *Nfsfs) populateDir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool) (errc int) {

	items, err := nfstarget.ReadDirPlus(path)
	if err != nil {
		log.Fatalln("Readdir error - ReadDirPlus():", err.Error())
		// try reconnecting
		err := connectNFS()
		if err != nil {
			log.Fatalln("Readdir error - ConnectNFS():", err.Error())
			return -fuse.ENOENT
		}

		items, err = nfstarget.ReadDirPlus(path)
		if err != nil {
			log.Fatalln("Readdir error - ReadDirPlus():", err.Error())
			return -fuse.ENOENT
		}
	}

	node := fs.openmap[path]

	if node == nil {
		node = fs.root
	}

	for _, item := range items {
		fs.ino++
		mode := uint32(item.Attr.Attr.Mode())
		if item.Attr.Attr.IsDir() {
			mode = fuse.S_IFDIR | (mode & 07777)
		} else {
			mode = fuse.S_IFREG | (mode & 0444)
		}

		child := newNode(0, fs.ino, mode, item.Attr.Attr.UID, item.Attr.Attr.GID)
		child.stat.Size = item.Attr.Attr.Size()

		if fill != nil {
			fill(item.FileName, &child.stat, 0)
		}

		if node.children[item.FileName] == nil {
			node.children[item.FileName] = child
		}
	}

	return 0
}

// Releasedir releases the specified directory by closing the node
func (fs *Nfsfs) Releasedir(path string, fh uint64) (errc int) {
	// log.Println("Releasedir() path: ", path)
	defer fs.synchronize()()
	return fs.closeNode(path)
}

// Chflags sets the flags on the specified node
func (fs *Nfsfs) Chflags(path string, flags uint32) (errc int) {

	defer fs.synchronize()()
	_, _, node := fs.lookupNode(path, nil)
	if nil == node {
		return -fuse.ENOENT
	}
	node.stat.Flags = flags
	node.stat.Ctim = fuse.Now()
	return 0
}

// evictModifiedFilesFromCacheLoop loops infinitely.
// Calls separate method to aid unit testing
func evictModifiedFilesFromCacheLoop() {

	for {
		evictModifiedFilesFromCache(getFileModTimeFromNFS)
		time.Sleep(time.Second * 10)
	}
}

// evictModifiedFilesFromCache removes any items from the cache
// if they've been modified since they were added.
func evictModifiedFilesFromCache(getFileModTime func(path string) time.Time) {
	for i := len(cachedNodes) - 1; i >= 0; i-- {
		cachedNode := cachedNodes[i]
		path := cachedNode.path

		modTime := getFileModTime(path)

		// see if file has been modified since we cached it
		if modTime.After(cachedNode.timeCached) {
			log.Println("Removing file from cache as modified time is more recent.", path, modTime)

			//TODO: Check deadlock possibility
			cachedNode.node.cache.lock.Lock()
			cachedNode.node.cache.byteRanges = []ByteRange{}
			cachedNode.node.cache.lock.Unlock()

			cachedNode.node.lock.Lock()
			cachedNode.node.data = []byte{}
			cachedNode.node.lock.Unlock()

			cachelock.Lock()
			cachedNodes = append(cachedNodes[:i], cachedNodes[i+1:]...)
			cachelock.Unlock()

			//TODO: Remove from local file system cache
			removeFileFromFSCache(path)

			broadcastCachedFileRemoved(path)
		}
	}
}

func getFileModTimeFromNFS(path string) time.Time {

	file, _, err := nfstarget.Lookup(path)
	if err != nil {
		log.Fatalln("NFS Lookup error. Will try reconnecting:", path, err.Error())
		// try reconnecting
		err := connectNFS()
		if err != nil {
			log.Fatalln("ConnectNFS():", err.Error())
		}

		file, _, err = nfstarget.Lookup(path)
		if err != nil {
			log.Fatalln("NFS Lookup error x2 - giving up.", path, err.Error())
		}
	}
	return file.ModTime()
}

// tryRemoteCache first checks if a file has been broadcast as
// cached by a remote server then requests the required data range
func tryRemoteCache(path string, fh uint64,
	node *Node, offset, endoffset int64, buff []byte) (numb int) {

	address, ok := remoteCachedFiles[path]
	if !ok {
		return 0
	}

	client, err := destConnection(address)
	if err != nil {
		log.Fatalln("Error creating destination connection to remote host: .", address, "\n\t", err.Error())
		return 0
	}
	defer client.Close()

	numb = getRemoteCacheData(path, fh, node, offset, endoffset, buff, client.Call)

	if numb > 0 {
		if len(node.cache.byteRanges) > 1 {
			go reduceFileCache(node, path)
		}
		if offset == 0 || endoffset == node.stat.Size {
			log.Println("Read() - remote cache hit:  ", offset, endoffset, len(buff), path)
		}
	}
	return
}

func getRemoteCacheData(filepath string, fh uint64, node *Node,
	offset, endoffset int64, buff []byte, rpcCall rpcCallFunc) (numb int) {

	// HACK: Filedata/buff shouldn't be needed here but
	// buffer comes through empty when added to response object
	request := &CachedDataRequest{
		Filepath:  filepath,
		Fh:        fh,
		Offset:    offset,
		Endoffset: endoffset,
		Filedata:  buff,
	}
	response := new(CachedDataResponse)
	//TODO: Figure out why response buffer doesn't make it to through RPC call
	// response.Filedata = buff

	err := rpcCall(getFileDataHandlerName, request, response)
	if err != nil {
		log.Fatalln("Error calling tryRemoteCache: ", filepath, err.Error())
	}

	if response.NumbBytes > 0 {
		// log.Println("4.1 RemoteCache GOT DATA.", filepath, response.NumbBytes)
		copy(buff, response.Filedata)
		node.cache.lock.Lock()
		node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: offset, high: endoffset})
		node.cache.lock.Unlock()
	} else {
		log.Println("4.2 RemoteCache NO DATA RECEIVED.", filepath)
	}
	return response.NumbBytes

}

// readFileFromNFS reads the specified file from the NFS server
// Should only be called once per file (assuming we have sufficient memory)
func readFileFromNFS(path string, node *Node, buff []byte, offset, endoffset int64) (numb int) {

	file, err := nfstarget.Open(path)
	if err != nil {
		log.Fatalln("Read - Open Error: ", err.Error())
		return 0
	}

	newoffset := offset
	for {
		_, err = file.Seek(newoffset, io.SeekStart)
		if err != nil {
			log.Fatalln("Read - Seek Error: ", err.Error())
			return 0
		}

		bytesread, err := file.Read(buff[numb:])

		if err == io.EOF {
			numb += bytesread
			break
		}

		if err != nil {
			//TODO: Error handling - what should we do here?
			log.Fatalln("Read - Error: ", err.Error())
			return 0
		}

		numb += bytesread
		newoffset += int64(bytesread)

		if numb == len(buff) {
			break
		}
	}
	return numb
}

// broadcastCachedFile informs any remote servers
// that a file has been cached in memory
func broadcastCachedFile(filepath string, fh uint64) {

	for _, address := range remoteServers {
		log.Println("broadcastCachedFile() - Address:", address, filepath, fh)

		client, err := destConnection(address)
		if err != nil {
			log.Fatalln("broadcastCachedFile() - Error creating destination connection to remote host: .", address, "\n\t", err.Error())
			os.Exit(2)
		}
		defer client.Close()

		request := &CacheUpdateRequest{Fromip: *thisip, Filepath: filepath, Fh: fh}
		response := new(CacheUpdateResponse)
		err = client.Call(fileCachedHandlerName, request, response)

		if err != nil {
			log.Fatalln("broadcastCachedFile() - Error calling FileCachedEvent: ", filepath, err.Error())
		}
	}
}

// broadcastCachedFileRemoved informs any remote servers
// that a file has been removed from the cache
func broadcastCachedFileRemoved(filepath string) {

	for _, address := range remoteServers {
		log.Println("broadcastCachedFileRemoved() - Address:", address, filepath)

		client, err := destConnection(address)
		if err != nil {
			log.Fatalln("broadcastCachedFileRemoved() - Error creating destination connection to remote host: .", address, "\n\t", err.Error())
			os.Exit(2)
		}
		defer client.Close()

		request := &CacheUpdateRequest{Filepath: filepath}
		response := new(CacheUpdateResponse)
		err = client.Call(fileRemovedFromCacheHandlerName, request, response)

		if err != nil {
			log.Fatalln("broadcastCachedFileRemoved() - Error calling FileRemovedFromCacheEvent: ", filepath, err.Error())
		}
	}
}

func destConnection(address string) (*rpc.Client, error) {

	client, err := rpc.Dial("tcp", address+":"+*rpcPort)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (fs *Nfsfs) getNode(path string, fh uint64) *Node {

	if fh == ^uint64(0) {
		_, _, node := fs.lookupNode(path, nil)
		return node
	}

	return fs.openmap[path]
}

func (fs *Nfsfs) openNode(path string, dir bool) (errc int, fh uint64) {

	_, _, node := fs.lookupNode(path, nil)

	if node == nil {
		log.Fatalln("openNode() - Node nil", path)
		return -fuse.ENOENT, ^uint64(0)
	}

	if !dir && node.stat.Mode&fuse.S_IFMT == fuse.S_IFDIR {
		log.Fatalln("openNode() - Node is meant to be file but isn't (is dir)", path)
		return -fuse.EISDIR, ^uint64(0)
	}

	if dir && node.stat.Mode&fuse.S_IFMT != fuse.S_IFDIR {
		log.Fatalln("openNode() - Node is meant to be dir but isn't (is file)", path)
		return -fuse.ENOTDIR, ^uint64(0)
	}

	node.opencnt++
	if node.opencnt == 1 {
		fs.openmap[path] = node
	}
	return 0, node.stat.Ino
}

func (fs *Nfsfs) closeNode(path string) int {
	node := fs.openmap[path]
	node.opencnt--
	return 0
}

func (fs *Nfsfs) lookupNode(path string, ancestor *Node) (parent *Node, name string, node *Node) {

	parent = fs.root
	node = fs.root
	name = ""

	for _, c := range strings.Split(path, "/") {
		if c != "" {
			if len(c) > 255 {
				log.Panicln(fuse.Error(-fuse.ENAMETOOLONG))
			}
			parent, name = node, c
			node = node.children[c]

			if node == nil {
				// log.Println("lookupNode() node nil, calling populateDir()", path, c)
				_ = fs.populateDir("/", nil)
				node = parent.children[c]
			}

			if node == ancestor && ancestor != nil {
				name = ""
				return
			}
		}
	}
	return
}

func newNode(dev uint64, ino uint64, mode uint32, uid uint32, gid uint32) *Node {

	timenow := fuse.Now()
	node := Node{
		stat: fuse.Stat_t{
			Dev:      dev,
			Ino:      ino,
			Mode:     mode,
			Nlink:    1,
			Uid:      uid,
			Gid:      gid,
			Atim:     timenow,
			Mtim:     timenow,
			Ctim:     timenow,
			Birthtim: timenow,
			Flags:    0,
		},
		cache: FileCache{byteRanges: []ByteRange{}},
	}

	if node.stat.Mode&fuse.S_IFMT == fuse.S_IFDIR {
		node.children = map[string]*Node{}
	}
	return &node
}

func resize(slice []byte, size int64, zeroinit bool) []byte {
	const allocunit = 64 * 1024
	allocsize := (size + allocunit - 1) / allocunit * allocunit
	if cap(slice) != int(allocsize) {
		var newslice []byte
		{
			defer func() {
				if r := recover(); nil != r {
					panic(fuse.Error(-fuse.ENOSPC))
				}
			}()
			newslice = make([]byte, size, allocsize)
		}
		copy(newslice, slice)
		slice = newslice
	} else if zeroinit {
		i := len(slice)
		slice = slice[:size]
		for ; len(slice) > i; i++ {
			slice[i] = 0
		}
	}
	return slice
}

func (fs *Nfsfs) synchronize() func() {
	fs.lock.Lock()
	return func() {
		fs.lock.Unlock()
	}
}

/// **RPC Handler methods** ///

// FileCachedEvent adds/updates the remoteCachedFiles map
func (h *RPCHandler) FileCachedEvent(req CacheUpdateRequest, res *CacheUpdateResponse) (err error) {

	if req.Filepath == "" {
		return errors.New("a file path must be specified")
	}

	//TODO: decide  whether locking is required here.
	// Probably not a big deal as stale reads are fine
	remoteCachedFiles[req.Filepath] = req.Fromip

	log.Println("FileCachedEvent - file:", req.Filepath, req.Fh, len(remoteCachedFiles))

	return
}

// FileCachedEvent adds/updates the remoteCachedFiles map
func (h *RPCHandler) FileRemovedFromCacheEvent(req CacheUpdateRequest, res *CacheUpdateResponse) (err error) {

	if req.Filepath == "" {
		return errors.New("a file path must be specified")
	}

	delete(remoteCachedFiles, req.Filepath)

	log.Println("FileRemovedFromCacheEvent - file:", req.Filepath, req.Fh, len(remoteCachedFiles))

	return
}

// GetFileData gets the requested byte range for the specified file
func (h *RPCHandler) GetFileData(req CachedDataRequest, res *CachedDataResponse) (err error) {

	// log.Println("2. GetFileData() - Buffer len: ", len(req.Filedata), req.Filepath, req.Fh)

	if req.Filepath == "" {
		return errors.New("a file path must be specified")
	}

	node := nfsfs.getNode(req.Filepath, req.Fh)
	if node == nil {
		return errors.New("node reference not found for " + req.Filepath + " FH:" + string(req.Fh))
	}

	if len(node.cache.byteRanges) == 0 {
		return errors.New("no cache data found for file: " + req.Filepath)
	}

	// HACK: Only way could get byte slice to come through correctly was
	// in the request so need to copy it over to the response here.
	res.Filedata = req.Filedata

	res.NumbBytes, _ = fetchLocalCacheData(req.Filepath, node, req.Offset, req.Endoffset, res.Filedata)
	// log.Println("2.1 GetFileData() - Ret data: ", res.NumbBytes)
	return
}

func getMemoryUsedMB() int {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int(m.Alloc / 1024 / 1024)
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func bToMb(b int64) int {
	return int(b / 1024 / 1024)
}
