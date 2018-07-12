package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/davefellows/cgofuse/fuse"
	"github.com/vmware/go-nfs-client/nfs"
	nfsrpc "github.com/vmware/go-nfs-client/nfs/rpc"
)

const (
	fileCachedHandlerName  = "RPCHandler.FileCachedEvent"
	getFileDataHandlerName = "RPCHandler.GetFileData"
)

var (
	nfshost   = flag.String("NFS-server", "localhost", "The hostname/IP for the NFS Server.")
	target    = flag.String("NFS-target", "/C/temp", "The target/path on the NFS Server.")
	mntpoint  = flag.String("mount-point", "", "Where to mount the file system.")
	thisip    = flag.String("ThisIP", "", "The IP address of this server for cache requests.")
	remoteips = flag.String("RemoteIPs", "", "Comma separate list of IPs for remote servers.")
	rpcPort   = flag.String("RemotePort", "5556", "Port to use for connection with remote servers.")

	nfsfs     *Nfsfs
	nfstarget *nfs.Target

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
	opencnt  int
	cache    FileCache
}

// FileCache represents in memory cached byte ranges
type FileCache struct {
	byteRanges []ByteRange
	lock       sync.Mutex
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

func main() {
	flag.Parse()

	log.Println("Remote host:", *nfshost)

	remoteCachedFiles = make(map[string]string)
	remoteServers = strings.Split(*remoteips, ",")

	fmt.Println("RemoteServers:", *remoteips, remoteServers)

	if len(remoteServers) > 0 {
		go setupRPCListener(*rpcPort)
	}

	err := connectNFS()
	if err != nil {
		os.Exit(1)
	}

	fmt.Println("Connected to NFS", *nfshost, *target)

	defer nfstarget.Close()

	nfsfs = newfs()
	host := fuse.NewFileSystemHost(nfsfs)
	host.Mount(*mntpoint, []string{})
}

func connectNFS() error {
	mount, err := nfs.DialMount(*nfshost)
	if err != nil {
		fmt.Println("unable to dial MOUNT service:", err.Error())
		return err
	}
	defer mount.Close()

	//TODO: Check if needed
	auth := nfsrpc.NewAuthUnix("", 1000, 1000)

	nfstarget, err = mount.Mount(*target, auth.Auth())
	if err != nil {
		fmt.Println("unable to mount volume:", err.Error())
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
	// fmt.Println("Open() path: ", path)

	defer fs.synchronize()()

	errc, fh = fs.openNode(path, false)
	if errc != 0 {
		fmt.Println("Open() - Error: ", errc)
	}
	fmt.Println("Open() path: ", path, fh)
	return
}

// Getattr gets the attributes for a specified node
func (fs *Nfsfs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	// fmt.Println("Getattr() path: ", path, fh)

	defer fs.synchronize()()

	node := fs.getNode(path, fh)
	if nil == node {
		// fmt.Println("Getattr() Error: nil node")
		return -fuse.ENOENT
	}

	*stat = node.stat
	return 0
}

// Getxattr gets the extended file attributes
func (fs *Nfsfs) Getxattr(path string, name string) (errc int, xattr []byte) {

	defer fs.synchronize()()

	_, _, node := fs.lookupNode(path, nil)

	if node == nil {
		return -fuse.ENOENT, nil
	}

	xattr, ok := node.xattr[name]
	if !ok {
		return -fuse.ENOATTR, nil
	}

	return 0, xattr
}

// Truncate truncates the specified file
func (fs *Nfsfs) Truncate(path string, size int64, fh uint64) (errc int) {
	fmt.Println("Truncate() path: ", path)

	defer fs.synchronize()()

	node := fs.getNode(path, fh)
	if nil == node {
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
		fmt.Println("Read() Error: nil node")
		return -fuse.ENOENT
	}

	endoffset := offset + int64(len(buff))
	if endoffset > node.stat.Size {
		endoffset = node.stat.Size
	}
	if endoffset < offset {
		return 0
	}
	// fmt.Println("\n1. Read() - offsets:\t\t\t", offset, endoffset, len(buff), path)

	// 1. See if we have this byte range cached locally in memory
	numb, newCacheItemRequired := fetchLocalCacheData(node, offset, endoffset, buff)

	if numb > 0 {
		if len(node.cache.byteRanges) > 1 {
			go reduceFileCache(node, path)
		}
		return numb
	}

	// 2. See if we have this file cached with one of our remotes
	numb = tryRemoteCache(path, fh, node, offset, endoffset, buff)

	if numb > 0 {
		if len(node.cache.byteRanges) > 1 {
			go reduceFileCache(node, path)
		}
	} else {
		// 3. Otherwise, let's get our file from NFS
		numb = readFileFromNFS(path, node, buff, offset, endoffset)
	}

	updateLocalCache(newCacheItemRequired, path, node, buff, offset, endoffset)

	node.stat.Atim = fuse.Now()
	return numb
}

// Release releases the specified file handle
func (fs *Nfsfs) Release(path string, fh uint64) (errc int) {
	// fmt.Println("Release() path: ", path, fh)

	defer fs.synchronize()()
	return fs.closeNode(path)
}

// Opendir opens the specified directory/node
func (fs *Nfsfs) Opendir(path string) (errc int, fh uint64) {
	// fmt.Println("Opendir() path: ", path)

	defer fs.synchronize()()
	return fs.openNode(path, true)
}

// Readdir populates the specified path's files and folders
func (fs *Nfsfs) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {
	// fmt.Println("Readdir() ", path)

	defer fs.synchronize()()

	return fs.populateDir(path, fill)
}

func (fs *Nfsfs) populateDir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool) (errc int) {

	dirs, err := nfstarget.ReadDirPlus(path)
	if err != nil {
		fmt.Println("Readdir error: ReadDirPlus()", err.Error())
		// try reconnecting
		err := connectNFS()
		if err != nil {
			fmt.Println("Readdir error: ConnectNFS()", err.Error())
			return -fuse.ENOENT
		}

		dirs, err = nfstarget.ReadDirPlus(path)
		if err != nil {
			fmt.Println("Readdir error: ReadDirPlus()", err.Error())
			return -fuse.ENOENT
		}
	}

	node := fs.openmap[path]

	if node == nil {
		node = fs.root
		fmt.Println("\tSetting node to fs.root")
	}

	fmt.Println("populateDir() - dirs:", len(dirs))

	for _, dir := range dirs {
		// fmt.Println("populateDir() - dir:", dir.FileName)
		fs.ino++
		mode := uint32(dir.Attr.Attr.Mode())
		if dir.Attr.Attr.IsDir() {
			mode = fuse.S_IFDIR | (mode & 07777)
		} else {
			mode = fuse.S_IFREG | (mode & 0444)
		}

		child := newNode(0, fs.ino, mode, dir.Attr.Attr.UID, dir.Attr.Attr.GID)
		child.stat.Size = dir.Attr.Attr.Size()

		if fill != nil {
			fill(dir.FileName, &child.stat, 0)
		}

		if node.children[dir.FileName] == nil {
			node.children[dir.FileName] = child
		}
	}

	return 0
}

// Releasedir releases the specified directory by closing the node
func (fs *Nfsfs) Releasedir(path string, fh uint64) (errc int) {
	// fmt.Println("Releasedir() path: ", path)
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

// updateLocalCache adds the new byte range to the in memory cache
func updateLocalCache(newByteRangeRequired bool, path string, node *Node, buff []byte, offset, endoffset int64) {

	if newByteRangeRequired {
		node.cache.lock.Lock()
		node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: offset, high: endoffset})
		node.cache.lock.Unlock()
	}

	if len(node.cache.byteRanges) > 1 {
		go reduceFileCache(node, path)
	}

	if int64(len(node.data)) < endoffset {
		// Resize for the entire file so we don't resize every time
		node.data = resize(node.data, node.stat.Size, false)
	}
	copy(node.data[offset:endoffset], buff)
}

// reduceFileCache merges byte ranges where possible.
// Should end up with only a single byte range per file.
func reduceFileCache(node *Node, filepath string) {

	var newByteRanges []ByteRange
	var lowest, highest int64

	node.cache.lock.Lock()
	sort.Slice(node.cache.byteRanges, func(i, j int) bool {
		return node.cache.byteRanges[i].low < node.cache.byteRanges[j].low
	})
	node.cache.lock.Unlock()

	for _, br := range node.cache.byteRanges {
		// 1. range extends the previous one above
		if br.low <= highest && br.high > highest {
			highest = br.high
			continue
		}

		// 2. range extends the previous one below
		if br.high >= lowest && br.low < lowest {
			lowest = br.low
			continue
		}
		// 3. If neither of the above then we need to keep this range
		newByteRanges = append(newByteRanges, ByteRange{low: lowest, high: highest})
		lowest = max(lowest, br.low)
		highest = max(highest, br.high)
	}

	// if we've made changes then append and update
	if highest != 0 {
		newByteRanges = append(newByteRanges, ByteRange{low: lowest, high: highest})

		node.cache.lock.Lock()
		node.cache.byteRanges = newByteRanges
		node.cache.lock.Unlock()
	}
	if highest-lowest == node.stat.Size {
		// fmt.Println("reduceFileCache CACHED file", filepath)
		if len(remoteServers) > 0 {
			broadcastCachedFile(remoteServers[0], filepath, node.stat.Ino)
		}
	}
}

// fetchLocalCacheData scans the in memory cache for the
// file and requested byte range. Copies to buff if found.
func fetchLocalCacheData(node *Node, offset, endoffset int64,
	buff []byte) (numBytes int, newCacheItemRequired bool) {

	cacheHit := false
	newCacheItemRequired = true
	// fmt.Println("fetchLocalCacheData() - ByteRanges: ", len(node.cache.byteRanges), offset, endoffset, len(buff))
	// fmt.Println("\nRead() - offsets:", endoffset-offset, len(buff), node.stat.Size, path)

	node.cache.lock.Lock()
	for _, br := range node.cache.byteRanges {
		if offset >= br.low && endoffset <= br.high {
			cacheHit = true
			break
		}

		if offset >= br.low && offset < br.high {
			// extend cache
			br.high = endoffset
			newCacheItemRequired = false
		}

		if endoffset < br.high && endoffset > br.low {
			// extend lower end of cache
			br.low = offset
			newCacheItemRequired = false
		}
	}
	node.cache.lock.Unlock()

	if cacheHit {
		fmt.Println("fetchLocalCacheData() - Cache Hit!", offset, endoffset, len(buff))
		copy(buff, node.data[offset:endoffset])
		numBytes = int(endoffset - offset)
	}
	return
}

// tryRemoteCache first checks if a file has been broadcast as
// cached by a remote server then requests the required data range
func tryRemoteCache(path string, fh uint64,
	node *Node, offset, endoffset int64, buff []byte) (numb int) {

	address, ok := remoteCachedFiles[path]
	if !ok {
		return 0
	}

	// fmt.Println("RemoteCache - ", path, len(buff), offset, endoffset)

	client, err := destConnection(address)
	if err != nil {
		fmt.Println("Error creating destination connection to remote host: .", address, "\n\t", err.Error())
		return 0
	}
	defer client.Close()

	return getRemoteCacheData(path, fh, node, offset, endoffset, buff, client.Call)
}

type rpcCallFunc func(method string, args interface{}, reply interface{}) error

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
		fmt.Println("Error calling tryRemoteCache: ", filepath, err.Error())
	}

	if response.NumbBytes > 0 {
		// fmt.Println("4.1 RemoteCache GOT DATA.", filepath, response.NumbBytes)
		copy(buff, response.Filedata)
		node.cache.lock.Lock()
		node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: offset, high: endoffset})
		node.cache.lock.Unlock()
	} else {
		fmt.Println("4.2 RemoteCache NO DATA RECEIVED.", filepath)
	}
	return response.NumbBytes

}

// readFileFromNFS reads the specified file from the NFS server
// Should only be called once per file (assuming we have sufficient memory)
func readFileFromNFS(path string, node *Node, buff []byte, offset, endoffset int64) (numb int) {

	file, err := nfstarget.Open(path)
	if err != nil {
		log.Fatal("Read - Open Error: ", err.Error())
		return 0
	}

	newoffset := offset
	for {
		_, err = file.Seek(newoffset, io.SeekStart)
		if err != nil {
			log.Fatal("Read - Seek Error: ", err.Error())
			return 0
		}

		bytesread, err := file.Read(buff[numb:])

		if err == io.EOF {
			numb += bytesread
			break
		}

		if err != nil {
			//TODO: Error handling - what should we do here?
			log.Fatal("Read - Error: ", err.Error())
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
func broadcastCachedFile(address, filepath string, fh uint64) {

	fmt.Println("broadcastCachedFile() - Address:", address, filepath, fh)

	client, err := destConnection(address)
	if err != nil {
		fmt.Println("broadcastCachedFile() - Error creating destination connection to remote host: .", address, "\n\t", err.Error())
		os.Exit(2)
	}
	defer client.Close()

	request := &CacheUpdateRequest{Fromip: *thisip, Filepath: filepath, Fh: fh}
	response := new(CacheUpdateResponse)
	err = client.Call(fileCachedHandlerName, request, response)

	if err != nil {
		fmt.Println("broadcastCachedFile() - Error calling FileCachedEvent: ", filepath, err.Error())
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
		return -fuse.ENOENT, ^uint64(0)
	}

	if !dir && node.stat.Mode&fuse.S_IFMT == fuse.S_IFDIR {
		return -fuse.EISDIR, ^uint64(0)
	}

	if dir && node.stat.Mode&fuse.S_IFMT != fuse.S_IFDIR {
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
	// Don't want to remove the node from our map
	// if 0 == node.opencnt {
	// 	delete(fs.openmap, node.stat.Ino)
	// }
	return 0
}

func (fs *Nfsfs) lookupNode(path string, ancestor *Node) (parent *Node, name string, node *Node) {

	parent = fs.root
	name = ""
	node = fs.root

	for _, c := range strings.Split(path, "/") {
		if c != "" {
			if len(c) > 255 {
				panic(fuse.Error(-fuse.ENAMETOOLONG))
			}
			parent, name = node, c
			node = node.children[c]

			if node == nil {
				fmt.Println("lookupNode() node nil, calling populateDir()", path, c)
				_ = fs.populateDir("/", nil)
				node = parent.children[c]
			}

			if node == ancestor && ancestor != nil {
				name = "" // special case loop condition
				return
			}
		}
	}
	return
}

func newNode(dev uint64, ino uint64, mode uint32, uid uint32, gid uint32) *Node {

	timenow := fuse.Now()
	node := Node{
		fuse.Stat_t{
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
		nil,
		nil,
		nil,
		0,
		FileCache{byteRanges: []ByteRange{}},
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

	fmt.Println("FileCachedEvent - file:", req.Filepath, req.Fh, len(remoteCachedFiles))

	return
}

// GetFileData gets the requested byte range for the specified file
func (h *RPCHandler) GetFileData(req CachedDataRequest, res *CachedDataResponse) (err error) {

	// fmt.Println("2. GetFileData() - Buffer len: ", len(req.Filedata), req.Filepath, req.Fh)

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

	res.NumbBytes, _ = fetchLocalCacheData(node, req.Offset, req.Endoffset, res.Filedata)
	// fmt.Println("2.1 GetFileData() - Ret data: ", res.NumbBytes)
	return
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
