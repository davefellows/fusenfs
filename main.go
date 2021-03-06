package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"path"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/billziss-gh/cgofuse/fuse"
)

const (
	fileCachedHandlerName           = "RPCHandler.FileCachedEvent"
	fileRemovedFromCacheHandlerName = "RPCHandler.FileRemovedFromCacheEvent"
	getFileDataHandlerName          = "RPCHandler.GetFileData"
	cacheDir                        = ".fusecache"
)

var (
	nfsmount = flag.String("nfs-mount", "", "The path where the NFS export has been mounted.")

	mntpoint    = flag.String("mount-point", "", "Where to mount the FUSE file system.")
	fscachepath = flag.String("fscachepath", "", "Path where to cache files to the filesystem.")
	thisip      = flag.String("thisip", "", "The IP address of this server for cache requests.")
	remoteips   = flag.String("remoteips", "", "Comma separate list of IPs for remote cache servers.")
	rpcPort     = flag.String("remoteport", "5555", "Port to use for connection with remote servers.")
	memLimit    = flag.Int("memlimitmb", 0, "Optionally limit how much memory can be used. Recommended, otherwise process will keep caching until something blows up.")
	logoutput   = flag.String("log", "stdout", "Set logging to stdout or a filename.")

	cpuprofile = flag.String("cpuprofile", "", "Optional: CPU Profile file to write.")

	nfsfs       *Nfsfs
	cachelock   sync.Mutex
	cachedNodes []CachedNode
	cachePath   string

	remoteServers     []string
	remoteCachedFiles map[string]string
	remoteCacheLock   sync.Mutex
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
	byteRanges []*ByteRange
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

	remoteCachedFiles = make(map[string]string)

	flag.Parse()

	if *logoutput != "stdout" {
		log.Println("Logging to file:", *logoutput)

		//TODO: Should ideally sanitize against path traversal
		// (though, this wouldn't ever) be exposed to malicious actors
		f, err := os.OpenFile(*logoutput, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			log.Fatalf("error opening file: %v - %v", *logoutput, err)
		}
		defer f.Close()
		log.SetOutput(f)
	} else {
		log.Println("Logging to stdout")
		log.SetOutput(os.Stdout)
	}

	if len(*remoteips) > 0 {
		remoteServers = strings.Split(*remoteips, ",")

		// remove this node's IP address if it's been included
		// in the list of nodes in the cluster/pool
		for i, ip := range remoteServers {
			if ip == *thisip {
				remoteServers = append(remoteServers[:i], remoteServers[i+1:]...)
				break
			}
		}
	}

	log.Println("NFS mount:", *nfsmount)
	log.Println("This node:", *thisip)
	log.Println("Remote servers:", len(remoteServers), remoteServers)
	log.Println("Remote port:", *rpcPort)
	log.Println("Memory limit:", *memLimit)

	if *cpuprofile != "" {
		perffile, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("CPU Profiling on.", perffile.Name())
		pprof.StartCPUProfile(perffile)
		defer pprof.StopCPUProfile()
	}

	if len(remoteServers) > 0 {
		go setupRPCListener(*rpcPort)
	}

	cachePath = path.Join(*fscachepath, cacheDir)
	setupLocalFSCache(cachePath)

	// start go routine to monitor for changes to files that are cached
	go evictModifiedFilesFromCacheLoop()

	nfsfs = newfs()
	host := fuse.NewFileSystemHost(nfsfs)
	log.Println("File system mounted at:", *mntpoint)
	host.Mount(*mntpoint, []string{})
	defer host.Unmount()
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
	fs.root = newNode(0, fs.ino, fuse.S_IFDIR|00555, 0, 0)
	fs.openmap = map[string]*Node{}
	return &fs
}

// Open opens the specified node
func (fs *Nfsfs) Open(path string, flags int) (errc int, fh uint64) {
	log.Println("Open() path: ", path)

	defer fs.synchronize()()

	errc, fh = fs.openNode(path, false)
	if errc != 0 {
		log.Println("Open() - Error: ", errc)
	}
	return
}

// Getattr gets the attributes for a specified node
func (fs *Nfsfs) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	log.Println("Getattr() path: ", path)

	defer fs.synchronize()()

	node := fs.getNode(path, fh)
	if nil == node {
		return -fuse.ENOENT
	}

	*stat = node.stat
	return 0
}

// Getxattr gets the extended file attributes
func (fs *Nfsfs) Getxattr(path string, name string) (errc int, xattr []byte) {
	// log.Println("Getxattr() path: ", path, name)

	defer fs.synchronize()()

	_, _, node := fs.lookupNode(path, nil)

	if node == nil {
		log.Println("Getxattr() Error: nil node")
		return -fuse.ENOENT, nil
	}

	xattr, ok := node.xattr[name]
	if !ok {
		log.Println("Getxattr() Error: not 'ok'", name)
		return -fuse.ENOATTR, nil
	}

	return 0, xattr
}

// Listxattr lists the extended attributes for the specified path
func (fs *Nfsfs) Listxattr(path string, fill func(name string) bool) (errc int) {
	fmt.Println("Listxattr() path: ", path)
	defer fs.synchronize()()
	_, _, node := fs.lookupNode(path, nil)
	if nil == node {
		return -fuse.ENOENT
	}
	for name := range node.xattr {
		if !fill(name) {
			return -fuse.ERANGE
		}
	}
	return 0
}

// Truncate truncates the specified file
func (fs *Nfsfs) Truncate(path string, size int64, fh uint64) (errc int) {
	log.Println("Truncate() path: ", path)

	defer fs.synchronize()()

	node := fs.getNode(path, fh)
	if nil == node {
		log.Println("Getxattr() - getNode(): node nil")
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
		numb = readFileFromLocalNFSMount(path, node, buff, offset, endoffset)
		if offset == 0 || endoffset == node.stat.Size {
			log.Println("Read() - read from NFS: \t", offset, endoffset, len(buff), path)
		}
		if numb > 0 {
			return numb
		}
	}

	// 1. See if we have this byte range cached locally in memory
	numb, newCacheItemRequired := fetchMemCacheData(path, node, offset, endoffset, buff)
	if numb > 0 {
		return numb
	}

	// 2.  Check if the file is cached on disk
	numb = fetchLocalFSCacheData(path, node, offset, endoffset, buff)
	if numb == 0 {
		// 3. See if we have this file cached with one of our remotes
		numb = tryRemoteCache(path, fh, node, offset, endoffset, buff)
	}

	if numb == 0 {
		// 4. Otherwise, get from NFS
		numb = readFileFromLocalNFSMount(path, node, buff, offset, endoffset)
		if offset == 0 || endoffset == node.stat.Size {
			log.Println("Read() - read from NFS: \t", offset, endoffset, len(buff), path)
		}
	}

	// go monitorForMissingByteRanges(path, node, brChan, doneChan)

	updateInMemoryCache(newCacheItemRequired, path, node, buff, offset, endoffset)

	node.stat.Atim = fuse.Now()
	return numb
}

// func monitorForMissingByteRanges(path string, node *Node, brChan <-chan ByteRange, doneChan <-chan bool) {

// 	var newByteRanges []ByteRange

// 	for {
// 		select {
// 		case <-doneChan:
// 			// retrieve byte ranges from NFS
// 			for _, br := range newByteRanges {
// 				log.Println("Getting missing BR:", br.low, br.high, path)

// 				newbuff := make([]byte, br.high-br.low+1)
// 				numb := readFileFromLocalNFSMount(path, node, newbuff, br.low, br.high)

// 				if numb > 0 {
// 					copy(node.data[br.low:br.high], newbuff)
// 					node.cache.lock.Lock()
// 					node.cache.byteRanges = append(node.cache.byteRanges, &ByteRange{low: br.low, high: br.high})
// 					node.cache.lock.Unlock()

// 				}
// 			}
// 			go reduceFileCache(node, path)
// 			return
// 		case br := <-brChan:
// 			log.Println("Missing byte range:", br.low, br.high, path)

// 			newByteRanges = append(newByteRanges, br)

// 			// newbuff := make([]byte, br.high-br.low+1)
// 			// numb := readFileFromLocalNFSMount(path, node, newbuff, br.low, br.high)

// 			// if numb > 0 {
// 			// 	copy(node.data[br.low:br.high], newbuff)

// 			// upload local cache but pass nil for channels so we don't risk
// 			// creating a mess of endless goroutines going around in circles!
// 			// updateLocalCache(true, path, node, newbuff, br.low, br.high, nil, nil, false)
// 			// }
// 		}
// 	}
// }

func calcOffsets(path string, node *Node, offset int64, buff []byte) (int64, int64) {
	endoffset := offset + int64(len(buff))
	if endoffset > node.stat.Size {
		endoffset = node.stat.Size
	}
	if endoffset < offset {
		return 0, 0
	}
	if offset == 0 {
		log.Println("Read() - start:\t\t", offset, endoffset, endoffset-offset, path, len(node.cache.byteRanges))
	}
	if endoffset == node.stat.Size {
		log.Println("Read() - end:  \t\t", offset, endoffset, endoffset-offset, path)
	}
	return offset, endoffset
}

// Release releases the specified file handle
func (fs *Nfsfs) Release(path string, fh uint64) (errc int) {
	log.Println("Release() path: ", path, fh)

	defer fs.synchronize()()
	return fs.closeNode(path)
}

// Opendir opens the specified directory/node
func (fs *Nfsfs) Opendir(path string) (errc int, fh uint64) {
	log.Println("Opendir() path: ", path)

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
func (fs *Nfsfs) populateDir(filepath string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool) (errc int) {

	fullpath := path.Join(*nfsmount, filepath)
	log.Println("populateDir() path: ", fullpath)

	fileinfos, err := ioutil.ReadDir(fullpath)
	if err != nil {
		log.Println("Readdir error ioutil.ReadDir:", err.Error())
		return -fuse.ENOENT
	}

	node := fs.openmap[filepath]

	if node == nil {
		log.Println("populateDir() nil node: ", filepath)
		node = fs.root
	}

	for _, fi := range fileinfos {
		// fs.ino++
		mode := uint32(fi.Mode())
		if fi.IsDir() {
			mode = fuse.S_IFDIR | (mode & 00555)
		} else {
			mode = fuse.S_IFREG | 00555
		}

		// getFileAttributes is os-dependent. Returns 0s on Windows
		ino, uid, gid := getFileAttributes(fi.Sys())
		// log.Println("populateDir() file: ", fi.Name(), mode, ino, uid, gid)

		child := newNode(0, ino, mode, uid, gid)
		child.stat.Size = fi.Size()

		if fill != nil {
			fill(fi.Name(), &child.stat, 0)
		}

		if node.children[fi.Name()] == nil {
			node.children[fi.Name()] = child
		}
	}
	return 0
}

// Releasedir releases the specified directory by closing the node
func (fs *Nfsfs) Releasedir(path string, fh uint64) (errc int) {
	log.Println("Releasedir() path: ", path)
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
	if len(cachedNodes) == 0 {
		return
	}
	// log.Println("CacheEviction. Len before:", len(cachedNodes))

	for i := len(cachedNodes) - 1; i >= 0; i-- {
		cachedNode := cachedNodes[i]
		filepath := cachedNode.path

		fullpath := path.Join(*nfsmount, filepath)
		modTime := getFileModTime(fullpath)

		// log.Println("CacheEviction. Node:", i, path, cachedNode.timeCached, modTime)

		// see if file has been modified since we cached it
		if modTime.After(cachedNode.timeCached) {
			log.Println("Removing file from cache as modified time is more recent.", filepath, modTime)

			//TODO: Check deadlock possibility
			cachedNode.node.cache.lock.Lock()
			cachedNode.node.cache.byteRanges = nil
			cachedNode.node.cache.lock.Unlock()

			cachedNode.node.lock.Lock()
			cachedNode.node.data = []byte{}
			cachedNode.node.lock.Unlock()

			cachelock.Lock()
			cachedNodes = append(cachedNodes[:i], cachedNodes[i+1:]...)
			cachelock.Unlock()

			//TODO: Remove from local file system cache
			removeFileFromFSCache(filepath)

			broadcastCachedFileRemoved(filepath)
		}
	}
	// log.Println("CacheEviction. Len after:", len(cachedNodes))
}

func getFileModTimeFromNFS(filepath string) time.Time {

	// fullpath := path.Join(*nfsmount, filepath)

	file, err := os.Stat(filepath)
	if err != nil {
		log.Println("getFileModTimeFromNFS() error for path:", filepath, err.Error())
	}
	return file.ModTime()
}

// tryRemoteCache first checks if the file has been broadcast as
// cached by a remote server then requests the required data range
func tryRemoteCache(path string, fh uint64,
	node *Node, offset, endoffset int64, buff []byte) (numb int) {

	address, ok := remoteCachedFiles[path]
	if !ok {
		return 0
	}

	client, err := destConnection(address)
	if err != nil {
		log.Println("Error creating destination connection to remote host: .", address, "\n\t", err.Error())
		return 0
	}
	defer client.Close()

	numb = getRemoteCacheData(path, fh, node, offset, endoffset, buff, client.Call)

	if numb > 0 && offset == 0 || endoffset == node.stat.Size {
		log.Println("Read() - remote cache hit:  ", offset, endoffset, len(buff), path)
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
	}
	response := new(CachedDataResponse)

	err := rpcCall(getFileDataHandlerName, request, response)
	if err != nil {
		log.Println("Error calling tryRemoteCache: ", filepath, err.Error())
	}

	if response.NumbBytes > 0 {
		// log.Println("4.1 RemoteCache GOT DATA.", filepath, response.NumbBytes)
		copy(buff, response.Filedata)
		updateCacheMetadataForNode(filepath, node, offset, endoffset)
	} else {
		log.Println("4.2 RemoteCache NO DATA RECEIVED.", filepath)
	}
	return response.NumbBytes

}

// readFileFromLocalNFSMount reads the specified file from the local NFS mount
// Should only be called once per byte range per file (assuming we have sufficient memory)
func readFileFromLocalNFSMount(filepath string, node *Node, buff []byte, offset, endoffset int64) (numb int) {

	newfile := path.Join(*nfsmount, filepath)

	if _, err := os.Stat(newfile); os.IsNotExist(err) {
		return 0
	}

	file, err := os.Open(newfile)
	if err != nil {
		log.Println("Error opening from NFS mount.", filepath, err)
		return 0
	}
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		log.Println("Error seeking on NFS mount.", filepath, offset, err)
		return 0
	}

	reader := bufio.NewReader(file)
	numb, err = reader.Read(buff)
	if err != nil {
		log.Println("Error reading from local NFS mount.", filepath, offset, err)
		return 0
	}
	// if numBytes > 0 && (offset == 0 || endoffset == node.stat.Size) {
	// 	log.Println("Read() - local FS cache hit:", offset, endoffset, len(buff), filepath)
	// }

	return numb
}

// broadcastCachedFile informs any remote servers
// that a file has been cached in memory
func broadcastCachedFile(filepath string, fh uint64) {

	for _, address := range remoteServers {
		log.Println("broadcastCachedFile() - Address:", address, filepath, fh)

		client, err := destConnection(address)
		if err != nil {
			log.Println("broadcastCachedFile() - Error creating destination connection to remote host: .", address, "\n\t", err.Error())
			return
		}
		defer client.Close()

		request := &CacheUpdateRequest{Fromip: *thisip, Filepath: filepath, Fh: fh}
		response := new(CacheUpdateResponse)
		err = client.Call(fileCachedHandlerName, request, response)

		if err != nil {
			log.Println("broadcastCachedFile() - Error calling FileCachedEvent: ", filepath, err.Error())
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
			log.Println("broadcastCachedFileRemoved() - Error creating destination connection to remote host: .", address, "\n\t", err.Error())
			return
		}
		defer client.Close()

		request := &CacheUpdateRequest{Filepath: filepath}
		response := new(CacheUpdateResponse)
		err = client.Call(fileRemovedFromCacheHandlerName, request, response)

		if err != nil {
			log.Println("broadcastCachedFileRemoved() - Error calling FileRemovedFromCacheEvent: ", filepath, err.Error())
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
	// log.Println("getNode() -", path)

	if fh == ^uint64(0) {
		_, _, node := fs.lookupNode(path, nil)
		return node
	}

	return fs.openmap[path]
}

func (fs *Nfsfs) openNode(path string, dir bool) (errc int, fh uint64) {
	log.Println("openNode() -", path)

	_, _, node := fs.lookupNode(path, nil)

	if node == nil {
		log.Println("openNode() - Node nil", path)
		return -fuse.ENOENT, ^uint64(0)
	}

	if !dir && node.stat.Mode&fuse.S_IFMT == fuse.S_IFDIR {
		log.Println("openNode() - Node is meant to be file but isn't (is dir)", path)
		return -fuse.EISDIR, ^uint64(0)
	}

	if dir && node.stat.Mode&fuse.S_IFMT != fuse.S_IFDIR {
		log.Println("openNode() - Node is meant to be dir but isn't (is file)", path)
		return -fuse.ENOTDIR, ^uint64(0)
	}

	node.opencnt++
	if node.opencnt == 1 {
		fs.openmap[path] = node
	}
	return 0, node.stat.Ino
}

func (fs *Nfsfs) closeNode(path string) int {
	// log.Println("closeNode() -", path)
	node := fs.openmap[path]
	node.opencnt--
	return 0
}

func (fs *Nfsfs) lookupNode(filepath string, ancestor *Node) (parent *Node, name string, node *Node) {
	// log.Println("lookupNode() -", filepath)

	parent = fs.root
	node = fs.root
	name = ""

	for _, c := range strings.Split(filepath, "/") {
		if c != "" {
			if len(c) > 255 {
				log.Panicln(fuse.Error(-fuse.ENAMETOOLONG))
			}
			parent, name = node, c
			node = node.children[c]

			if node == nil {
				log.Println("lookupNode() node nil, calling populateDir()", filepath)
				//TODO: Optimize this
				_, _ = fs.openNode(path.Dir(filepath), true)
				_ = fs.populateDir(path.Dir(filepath), nil)
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
		cache: FileCache{},
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

	remoteCacheLock.Lock()
	remoteCachedFiles[req.Filepath] = req.Fromip
	remoteCacheLock.Unlock()

	log.Println("FileCachedEvent - file:", req.Filepath, req.Fh, len(remoteCachedFiles))

	return
}

// FileRemovedFromCacheEvent adds/updates the remoteCachedFiles map
func (h *RPCHandler) FileRemovedFromCacheEvent(req CacheUpdateRequest, res *CacheUpdateResponse) (err error) {

	if req.Filepath == "" {
		return errors.New("a file path must be specified")
	}

	remoteCacheLock.Lock()
	delete(remoteCachedFiles, req.Filepath)
	remoteCacheLock.Unlock()

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

	len := req.Endoffset - req.Offset
	res.Filedata = make([]byte, len)

	res.NumbBytes, _ = fetchMemCacheData(req.Filepath, node, req.Offset, req.Endoffset, res.Filedata)
	// log.Println("2.1 GetFileData() - Ret data: ", res.NumbBytes)
	return
}

func getMemoryUsedMB() int {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int(m.Alloc / 1024 / 1024)
}

func bToMb(b int64) int {
	return int(b / 1024 / 1024)
}
