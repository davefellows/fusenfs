package main

import (
	"bufio"
	"log"
	"os"
	"os/user"
	"path"
)

func createLocalCacheDir() (cachePath string) {
	// get the user's current home directory
	usr, err := user.Current()
	if err != nil {
		log.Fatalln(err)
	}
	cachePath = path.Join(usr.HomeDir, cacheDir)
	// create cache dir if doesn't already exist
	//TODO: reduce permissions to cache directory
	err = os.MkdirAll(cachePath, 0777)
	if err != nil {
		log.Panicln(err)
	}
	log.Println("Created local filesystem cache dir:", cachePath)
	return cachePath
}

func writeFileToFilesystem(filepath string, node *Node) {

	newfile := path.Join(cachePath, filepath)

	log.Println("Write file to FS cache.", newfile)

	// open output file
	fout, err := os.Create(newfile)
	if err != nil {
		log.Fatalln("Error creating local cache file.", err)
		return
	}

	defer func() {
		if err = fout.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	w := bufio.NewWriter(fout)
	if _, err := w.Write(node.data); err != nil {
		log.Fatalln("Error writing to local cache file.", err)
	}
}

func removeFileFromFSCache(filepath string) {

	filetodelete := path.Join(cachePath, filepath)

	err := os.Remove(filetodelete)
	if err != nil {
		log.Println("Error deleting local cache file.", filepath, err)
	}
}

func fetchLocalFSCacheData(filepath string, node *Node, offset, endoffset int64,
	buff []byte) (numBytes int) {

	newfile := path.Join(cachePath, filepath)

	if _, err := os.Stat(newfile); os.IsNotExist(err) {
		return 0
	}

	file, err := os.Open(newfile)
	if err != nil {
		log.Println("Error opening local cache file.", filepath, err)
		return 0
	}
	_, err = file.Seek(offset, 0)
	if err != nil {
		log.Println("Error seeking on local cache file.", filepath, offset, err)
		return 0
	}

	reader := bufio.NewReader(file)
	numBytes, err = reader.Read(buff)
	if err != nil {
		log.Println("Error reading from local cache file.", filepath, err)
		return 0
	}
	if numBytes > 0 && (offset == 0 || endoffset == node.stat.Size) {
		log.Println("Read() - local FS cache hit:", offset, endoffset, len(buff), filepath)
	}

	return numBytes
}
