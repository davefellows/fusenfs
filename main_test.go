package main

import (
	"testing"
	"time"
)

func TestReadCallsLocalCache(t *testing.T) {

	fs := newfs()
	node := createTestNode()
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 0, high: 5})
	node.data = []byte{0, 1, 2, 3, 4}
	node.stat.Size = 5

	buff := make([]byte, 5)

	filepath := "file"
	fs.root.children[filepath] = node
	fs.Open(filepath, 0)
	fs.Read(filepath, buff, 0, 1)

	for i := 0; i < 5; i++ {
		if buff[i] != byte(i) {
			t.Error("Invalid data returned from cache -", i, buff[i])
		}
	}
}

func TestReadWithOffsetCallsLocalCache(t *testing.T) {

	fs := newfs()
	node := createTestNode()
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 0, high: 10})
	node.data = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	node.stat.Size = 10

	buff := make([]byte, 5)

	filepath := "file"
	fs.root.children[filepath] = node
	fs.Open(filepath, 0)
	// use offset of 3
	fs.Read(filepath, buff, 3, 1)

	for i := 0; i < 5; i++ {
		if buff[i] != byte(i+3) {
			t.Error("Invalid data returned from cache -", i, buff[i])
		}
	}

}

func TestReadWithRemoteCacheFetchOverRPC(t *testing.T) {

	go setupRPCListener("5555")

	remoteServers = []string{"localhost"}
	filepath := "file"
	remoteCachedFiles = make(map[string]string)
	remoteCachedFiles[filepath] = "localhost"

	node := createTestNode()
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 0, high: 5})
	node.data = []byte{0, 1, 2, 3, 4}
	node.stat.Size = 5

	buff := make([]byte, 5)

	nfsfs = newfs()
	nfsfs.root.children[filepath] = node
	nfsfs.Open(filepath, 0)

	tryRemoteCache(filepath, 1, node, 0, 5, buff)

	for i := 0; i < 5; i++ {
		if buff[i] != byte(i) {
			t.Error("Invalid data returned from cache -", i, buff[i])
		}
	}
}

func TestCanFetchFromLocalCache(t *testing.T) {

	node := createTestNode()
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 0, high: 5})

	buff := make([]byte, 5)
	node.data = []byte{0, 1, 2, 3, 4}
	numb, extendCacheItem := fetchLocalCacheData("file", node, 0, 5, buff)

	for i := 0; i < 5; i++ {
		if buff[i] != byte(i) {
			t.Error("Invalid data returned from cache -", i, buff[i])
		}
	}

	if numb != 5 {
		t.Error("Number of bytes read from cache should be 5")
	}

	if extendCacheItem {
		t.Error("extendCacheItem should be false")
	}
}

func TestCanExpandLocalCacheHigh(t *testing.T) {

	node := createTestNode()
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 0, high: 5})

	buff := make([]byte, 1)
	node.data = buff
	numb, extendCacheItem := fetchLocalCacheData("file", node, 0, 10, buff)

	if !extendCacheItem {
		t.Error("extendCacheItem should be true")
	}

	if node.cache.byteRanges[0].high != 5 {
		t.Error("ByteRange high should 5, not: ", node.cache.byteRanges[0].high)
	}

	if numb != 0 {
		t.Error("Number of bytes read from cache should be 0, not: ", numb)
	}
}

func TestCanExpandLocalCacheLow(t *testing.T) {

	node := createTestNode()
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 5, high: 10})

	buff := make([]byte, 1)
	node.data = buff
	numb, extendCacheItem := fetchLocalCacheData("file", node, 0, 7, buff)

	if !extendCacheItem {
		t.Error("extendCacheItem should be true")
	}

	if node.cache.byteRanges[0].low != 5 {
		t.Error("ByteRange high should 5, not: ", node.cache.byteRanges[0].low)
	}

	if numb != 0 {
		t.Error("Number of bytes read from cache should be 0, not: ", numb)
	}
}

func TestReduceCache(t *testing.T) {
	node := createTestNode()
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 0, high: 10})
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 10, high: 20})
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 20, high: 30})

	reduceFileCache(node, "")

	if len(node.cache.byteRanges) != 1 {
		t.Error("Expected one byte range after reduction. Got", len(node.cache.byteRanges))
	}

	low := node.cache.byteRanges[0].low
	high := node.cache.byteRanges[0].high
	if low != 0 || high != 30 {
		t.Error("Expected low=0, high=30. Got", low, high)
	}
}

func TestReduceCacheOutOfOrder(t *testing.T) {
	node := createTestNode()
	// byte ranges out of order
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 10, high: 20})
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 20, high: 30})
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 0, high: 10})

	reduceFileCache(node, "")

	if len(node.cache.byteRanges) != 1 {
		t.Error("Expected one byte range after reduction. Got", len(node.cache.byteRanges))
	}

	low := node.cache.byteRanges[0].low
	high := node.cache.byteRanges[0].high
	if low != 0 || high != 30 {
		t.Error("Expected low=0, high=30. Got", low, high)
	}
}

func TestReduceCacheOutOfOrderWithGaps(t *testing.T) {
	node := createTestNode()
	// byte ranges out of order
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 10, high: 20})
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 20, high: 30})
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 0, high: 10})
	// Gap
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 40, high: 50})
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 45, high: 60})
	// Gap
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 61, high: 70})

	reduceFileCache(node, "")

	for _, br := range node.cache.byteRanges {
		t.Log(br)
	}

	if len(node.cache.byteRanges) != 3 {
		t.Error("Expected 3 byte ranges after reduction. Got", len(node.cache.byteRanges))
	}

}

func TestUpdateLocalCache(t *testing.T) {
	node := createTestNode()

	buff := make([]byte, 5)
	node.data = buff
	node.stat.Size = 10

	// Use an endoffset (6) that is larger than the current
	// node.data length but less than node.stat.Size
	updateLocalCache(true, "", node, buff, 0, 6)

	if len(node.cache.byteRanges) != 1 {
		t.Error("Expected one byte range after reduction. Got", len(node.cache.byteRanges))
	}

	if int64(len(node.data)) != node.stat.Size {
		t.Error("Expected node.data length to be equal to node.stat.Size. ", len(node.data), node.stat.Size)
	}
}

func TestUpdateLocalCacheNoNewByteRange(t *testing.T) {
	node := createTestNode()

	buff := make([]byte, 5)
	node.data = buff
	node.stat.Size = 10

	// Use an endoffset (6) that is larger than the current
	// node.data length but less than node.stat.Size
	updateLocalCache(false, "", node, buff, 0, 6)

	if len(node.cache.byteRanges) != 0 {
		t.Error("Expected no byte rangse after reduction. Got", len(node.cache.byteRanges))
	}

	if int64(len(node.data)) != node.stat.Size {
		t.Error("Expected node.data length to be equal to node.stat.Size. ", len(node.data), node.stat.Size)
	}
}

func TestUpdateLocalCacheNoExtendNodeData(t *testing.T) {
	node := createTestNode()

	buff := make([]byte, 5)
	node.data = buff
	node.stat.Size = 10

	// Use an endoffset (5) that same as current node.data length
	updateLocalCache(false, "", node, buff, 0, 5)

	if len(node.cache.byteRanges) != 0 {
		t.Error("Expected no byte rangse after reduction. Got", len(node.cache.byteRanges))
	}

	if len(node.data) != 5 {
		t.Error("Expected node.data length to still be 5. ", len(node.data))
	}
}

func TestRemoteCacheFetch(t *testing.T) {
	var offset, endoffset int64
	file, fh := "file", uint64(1)
	buff := make([]byte, 5)
	node := createTestNode()

	numbbytes := getRemoteCacheData(file, fh, node, offset, endoffset, buff, mockRPCCall)

	if numbbytes == 0 {
		t.Error("No data returned. Expected 5 bytes")
	}

	for i := 0; i < 5; i++ {
		if buff[i] != byte(i) {
			t.Error("Invalid data returened from cache -", i, buff[i])
		}
	}
}

func TestRemoteCacheFetchWithNoCacheList(t *testing.T) {
	// should return immediately as remoteCachedFiles map is empty
	numbbytes := tryRemoteCache("", 0, nil, 0, 0, nil)

	if numbbytes != 0 {
		t.Error("Expecting number of bytes to be 0. Got ", numbbytes)
	}
}

func TestLocalCacheAfterRemoteCacheFetch(t *testing.T) {
	//TODO: TestLocalCacheAfterRemoteCacheFetch
	// Hard to test on a single machine as the same data structures are used!
}

func TestMemoryLimit(t *testing.T) {
	nodeThatShouldBeRemoved := createTestNode()
	nodes := []*Node{nodeThatShouldBeRemoved, createTestNode(), createTestNode()}

	cacheNodes := []CachedNode{
		CachedNode{node: nodes[0], lastAccessed: time.Now()},
		CachedNode{node: nodes[1], lastAccessed: time.Now()},
		CachedNode{node: nodes[2], lastAccessed: time.Now()},
	}

	nodeThatShouldBeRemoved.cache.byteRanges = []ByteRange{ByteRange{}}
	nodeThatShouldBeRemoved.data = []byte{0, 1, 2}

	cachedNodes = cacheNodes
	findAndRemoveEarliestCacheItems(10)

	if len(nodeThatShouldBeRemoved.data) > 0 {
		t.Error("Expecting node's data to be empty/zero length. Len:", len(nodeThatShouldBeRemoved.data))
	}

	if len(nodeThatShouldBeRemoved.cache.byteRanges) > 0 {
		t.Error("Expecting node's byteRanges to be empty/zero length. Len:", len(nodeThatShouldBeRemoved.cache.byteRanges))
	}

}

func TestMemoryLimitWithModifiedNode(t *testing.T) {
	nodeThatShouldBeRemoved := createTestNode()
	nodes := []*Node{createTestNode(), createTestNode(), nodeThatShouldBeRemoved}

	cacheNodes := []CachedNode{
		CachedNode{node: nodes[0], lastAccessed: time.Now()},
		CachedNode{node: nodes[1], lastAccessed: time.Now()},
		CachedNode{node: nodes[2], lastAccessed: time.Now()},
	}

	// Update times of first two nodes.
	// 3rd node should now be least-recently accessed and removed
	nodes[0].cache.cachedNode.lastAccessed = time.Now()
	nodes[1].cache.cachedNode.lastAccessed = time.Now()

	nodeThatShouldBeRemoved.cache.byteRanges = []ByteRange{ByteRange{}}
	nodeThatShouldBeRemoved.data = []byte{0, 1, 2}

	cachedNodes = cacheNodes
	findAndRemoveEarliestCacheItems(10)

	if len(nodeThatShouldBeRemoved.data) > 0 {
		t.Error("Expecting node's data to be empty/zero length. Len:", len(nodeThatShouldBeRemoved.data))
	}

	if len(nodeThatShouldBeRemoved.cache.byteRanges) > 0 {
		t.Error("Expecting node's byteRanges to be empty/zero length. Len:", len(nodeThatShouldBeRemoved.cache.byteRanges))
	}

}

func mockRPCCall(method string, args interface{}, reply interface{}) error {

	if resp, ok := reply.(*CachedDataResponse); ok {
		resp.Filedata = []byte{0, 1, 2, 3, 4}
		resp.NumbBytes = 5
	}
	return nil
}

func createTestNode() *Node {
	return &Node{
		cache:    FileCache{},
		children: make(map[string]*Node),
	}
}
