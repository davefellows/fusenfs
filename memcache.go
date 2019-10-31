package main

import (
	"log"
	"math"
	"runtime"
	"sort"
	"time"
)

// updateInMemoryCache adds the new byte range to the in memory cache
func updateInMemoryCache(newByteRangeRequired bool, path string, node *Node, buff []byte, offset, endoffset int64) {

	if int64(len(node.data)) < endoffset {
		// Resize for the entire file so we don't resize every time which ends up expensive
		node.data = resize(node.data, node.stat.Size, false)
	}

	// Check memory usage if memLimit has been set
	if *memLimit != 0 {
		memUsedMB := getMemoryUsedMB()
		if memUsedMB > *memLimit {
			log.Println("Freeing memory. Memory Used MB:", memUsedMB, " Limit:", *memLimit)
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
// Should end up with only a single byte range per file (assuming client app is eventually reading all data).
// Important to note when determining contiguity; byte ranges are inclusive
func reduceFileCache(node *Node, filepath string) {

	var newByteRanges []*ByteRange
	var lowest, highest int64

	// sort the byte ranges slice first so we can see which ranges are contiguous
	node.cache.lock.Lock()
	sort.Slice(node.cache.byteRanges, func(i, j int) bool {
		return node.cache.byteRanges[i].low < node.cache.byteRanges[j].low
	})

	var count int64

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

		if br.high <= highest {
			continue
		}

		// 3. If neither of the above then we need to keep the prior range
		newByteRanges = append(newByteRanges, &ByteRange{low: lowest, high: highest})
		count += highest - lowest

		// This is max() rather than min() as to get here we must have gapped up (i.e. missing byte range)
		// Alternatively, should be able to just set lowest to br.low (TODO: need to test)
		lowest = max(lowest, br.low)
		highest = max(highest, br.high)
	}

	// if we've made changes then append and update
	if highest != 0 {
		newByteRanges = append(newByteRanges, &ByteRange{low: lowest, high: highest})
		count += highest - lowest

		node.cache.byteRanges = newByteRanges
	}
	node.cache.lock.Unlock()

	log.Println("reduceFileCache() - ", len(node.cache.byteRanges), lowest, highest, highest-lowest, count, node.stat.Size, filepath)

	if highest-lowest == node.stat.Size {
		// log.Println("reduceFileCache CACHED file", filepath)
		broadcastCachedFile(filepath, node.stat.Ino)

		// Write file to local file system
		go writeFileToFilesystem(filepath, node)
	}
}

// fetchMemCacheData scans the in memory cache for the
// file and requested byte range. Copies to buff if found.
func fetchMemCacheData(path string, node *Node, offset, endoffset int64, buff []byte) (numBytes int, newCacheItemRequired bool) {

	cacheHit := false
	newCacheItemRequired = true

	node.cache.lock.Lock()
	for _, br := range node.cache.byteRanges {
		if offset >= br.low && endoffset <= br.high {
			cacheHit = true
			break
		}

		if offset >= br.low && offset < br.high {
			log.Println("fetchMemCacheData() - extend cache up", offset, endoffset, br.low, br.high, path)
			// extend higher end of cached range
			br.high = endoffset
			newCacheItemRequired = false
		}

		if endoffset < br.high && endoffset > br.low {
			log.Println("fetchMemCacheData() - extend cache down", offset, endoffset, br.low, br.high, path)
			// extend lower end of cached range
			br.low = offset
			newCacheItemRequired = false
		}
	}
	node.cache.lock.Unlock()

	if cacheHit {
		copy(buff, node.data[offset:endoffset])
		numBytes = int(endoffset - offset)

		//NOTE: Commenting this out as it's probably redundant... Need to test
		//********************************************************************
		// if len(node.cache.byteRanges) > 1 {
		// 	// log.Println("Reduce file cache:", path, len(node.cache.byteRanges))
		// 	go reduceFileCache(node, path, brChan, doneChan)
		// }
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

	// sort the cached nodes collection so the oldest items appear first
	cachelock.Lock()
	sort.Slice(cachedNodes, func(i, j int) bool {
		return cachedNodes[i].lastAccessed.Before(cachedNodes[j].lastAccessed)
	})

	log.Println("CachedItems - Before:", len(cachedNodes))

	var freedMem float64 = 0
	count := 0
	for i := 0; i < len(cachedNodes); i++ {
		// convert to MB
		size := float64(len(cachedNodes[0].node.data)) / 1024 / 1024
		// don't bother removing if it's < 500KB
		if size < 0.5 {
			// log.Printf("CachedItem skipped: %v, Size: %v MB \n", cachedNodes[i].path, math.Round(size*100)/100)
			continue
		}

		log.Printf("CachedItem removed: %v, Size: %v MB \n", cachedNodes[i].path, math.Round(size*100)/100)

		freedMem += size
		count++

		cachedNodes[i].node.cache.lock.Lock()
		cachedNodes[i].node.cache.byteRanges = nil
		cachedNodes[i].node.cache.lock.Unlock()
		cachedNodes[i].node.data = []byte{}

		if int(freedMem) >= memToFreeMB {
			break
		}
	}

	cachedNodes = cachedNodes[count:]
	log.Printf("CachedItems - After: %v, number removed: %v\n", len(cachedNodes), count)
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
