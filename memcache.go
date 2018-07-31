package main

import (
	"log"
	"runtime"
	"sort"
	"time"
)

// updateLocalCache adds the new byte range to the in memory cache
func updateLocalCache(newByteRangeRequired bool, path string, node *Node, buff []byte, offset, endoffset int64) {

	if int64(len(node.data)) < endoffset {
		// Resize for the entire file so we don't resize every time
		node.data = resize(node.data, node.stat.Size, false)
	}

	// Check memory usage if memLimit has been set
	if *memLimit != 0 {
		memUsedMB := getMemoryUsedMB()
		if memUsedMB > *memLimit {
			log.Println("Memory Used MB:", memUsedMB, "Limit:", *memLimit)
			// remove least recently accessed items from cache
			freeMemory(memUsedMB - *memLimit)
		}
	}

	copy(node.data[offset:endoffset], buff)

	if newByteRangeRequired {
		updateCacheMetadataForNode(path, node, offset, endoffset)
	}

}

func updateCacheMetadataForNode(path string, node *Node, offset, endoffset int64) {

	// log.Println("Update cache metadata:", path, len(node.cache.byteRanges))
	if len(node.cache.byteRanges) == 0 {
		// If first byteRange then add to list of cached items by time
		node.cache.cachedNode = CachedNode{
			path:         path,
			node:         node,
			timeCached:   time.Now(),
			lastAccessed: time.Now(),
		}
		cachelock.Lock()
		cachedNodes = append(cachedNodes, node.cache.cachedNode)
		cachelock.Unlock()
	} else {
		// Update last access time so we evict items with the oldest previous access
		node.cache.cachedNode.lastAccessed = time.Now()
	}
	node.cache.lock.Lock()
	node.cache.byteRanges = append(node.cache.byteRanges, &ByteRange{low: offset, high: endoffset})
	node.cache.lock.Unlock()
	go reduceFileCache(node, path)
}

// reduceFileCache merges byte ranges where possible.
// Should end up with only a single byte range per file.
func reduceFileCache(node *Node, filepath string) {

	var newByteRanges []*ByteRange
	var lowest, highest int64

	node.cache.lock.Lock()
	sort.Slice(node.cache.byteRanges, func(i, j int) bool {
		return node.cache.byteRanges[i].low < node.cache.byteRanges[j].low
	})
	node.cache.lock.Unlock()
	var count int64
	for _, br := range node.cache.byteRanges {
		count += br.high - br.low
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
		newByteRanges = append(newByteRanges, &ByteRange{low: lowest, high: highest})
		// This is max() rather than min() as to get here we must have gapped up
		// Alternatively, could probably just set lowest to br.low (TODO: test)
		lowest = max(lowest, br.low)
		highest = max(highest, br.high)
	}

	// if we've made changes then append and update
	if highest != 0 {
		newByteRanges = append(newByteRanges, &ByteRange{low: lowest, high: highest})

		node.cache.lock.Lock()
		node.cache.byteRanges = newByteRanges
		node.cache.lock.Unlock()
	}
	log.Println("reduceFileCache() - ", len(node.cache.byteRanges), lowest, highest, highest-lowest, count, node.stat.Size)
	//TODO: Need a more exact count!
	if highest-lowest == node.stat.Size {
		// log.Println("reduceFileCache CACHED file", filepath)
		broadcastCachedFile(filepath, node.stat.Ino)

		// Write file to local file system
		go writeFileToFilesystem(filepath, node)
	}
}

// fetchLocalCacheData scans the in memory cache for the
// file and requested byte range. Copies to buff if found.
func fetchMemCacheData(path string, node *Node, offset, endoffset int64,
	buff []byte) (numBytes int, newCacheItemRequired bool) {

	cacheHit := false
	newCacheItemRequired = true

	node.cache.lock.Lock()
	for _, br := range node.cache.byteRanges {
		if offset >= br.low && endoffset <= br.high {
			cacheHit = true
			break
		}

		if offset >= br.low && offset < br.high {
			log.Println("fetchMemCacheData() - extend cache up", offset, endoffset, br.low, br.high)
			// extend cache
			br.high = endoffset
			newCacheItemRequired = false
		}

		if endoffset < br.high && endoffset > br.low {
			log.Println("fetchMemCacheData() - extend cache down", offset, endoffset, br.low, br.high)
			// extend lower end of cache
			br.low = offset
			newCacheItemRequired = false
		}
	}
	node.cache.lock.Unlock()

	if cacheHit {
		copy(buff, node.data[offset:endoffset])
		numBytes = int(endoffset - offset)

		if len(node.cache.byteRanges) > 1 {
			// log.Println("Reduce file cache:", path, len(node.cache.byteRanges))
			go reduceFileCache(node, path)
		}
		if offset == 0 || endoffset == node.stat.Size {
			log.Println("Read() - in-memory cache hit:", offset, endoffset, len(buff), path)
		}

	}
	return
}

func freeMemory(memToFreeMB int) {

	if len(cachedNodes) <= 1 {
		return
	}

	cachelock.Lock()
	sort.Slice(cachedNodes, func(i, j int) bool {
		return cachedNodes[i].lastAccessed.Before(cachedNodes[j].lastAccessed)
	})

	log.Println("CachedItems - Before:", len(cachedNodes))

	freedMem := 0
	count := 0
	for i := 0; i < len(cachedNodes); i++ {
		freedMem += len(cachedNodes[0].node.data)
		count++

		cachedNodes[i].node.cache.lock.Lock()
		//TODO: Check this change still frees memory
		cachedNodes[i].node.cache.byteRanges = nil
		cachedNodes[i].node.cache.lock.Unlock()
		cachedNodes[i].node.data = []byte{}

		if freedMem >= memToFreeMB {
			break
		}
	}

	cachedNodes = cachedNodes[count:]
	log.Println("CachedItems - After:", len(cachedNodes), count)
	cachelock.Unlock()

	// Call GC
	runtime.GC()
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
