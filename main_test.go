package main

import (
	"testing"
)

func TestCanFetchFromLocalCache(t *testing.T) {

	node := createTestNode()
	node.cache.byteRanges = append(node.cache.byteRanges, ByteRange{low: 0, high: 5})

	buff := make([]byte, 5)
	node.data = []byte{0, 1, 2, 3, 4}
	numb, extendCacheItem := fetchLocalCacheData(node, 0, 5, buff)

	for i := 0; i < 5; i++ {
		if buff[i] != byte(i) {
			t.Error("Invalid data returened from cache -", i, buff[i])
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
	numb, extendCacheItem := fetchLocalCacheData(node, 0, 10, buff)

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
	numb, extendCacheItem := fetchLocalCacheData(node, 0, 7, buff)

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

func createTestNode() *Node {
	return &Node{
		cache:    FileCache{},
		children: make(map[string]*Node),
	}
}
