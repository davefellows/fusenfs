package main

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"
	"os/user"
	"path"
	"time"
)

func setupLocalFSCache(cacheDir string) (cachePath string) {
	// get the user's current home directory
	usr, err := user.Current()
	if err != nil {
		log.Fatalln(err)
	}
	cachePath = path.Join(usr.HomeDir, cacheDir)
	// create cache dir if doesn't already exist
	err = os.MkdirAll(cachePath, 0700)
	if err != nil {
		log.Panicln(err)
	}
	log.Println("Created local filesystem cache dir:", cachePath)

	// remove changed files
	_ = deleteLocalCacheFilesIfModified(cachePath, *nfsmount, getFileModTimeFromNFS)

	return cachePath
}

func deleteLocalCacheFilesIfModified(cachePath, nfsPath string, getFileModTime func(path string) time.Time) (removedFiles []string) {

	fileinfos, err := ioutil.ReadDir(cachePath)
	if err != nil {
		log.Println("deleteLocalCacheFilesIfModified() - error:", err.Error())
	}

	for _, fi := range fileinfos {
		fullpath := path.Join(cachePath, fi.Name())
		nfsfullpath := path.Join(nfsPath, fi.Name())

		if fi.IsDir() {
			// recursively call for any subdirectories
			files := deleteLocalCacheFilesIfModified(fullpath, nfsfullpath, getFileModTime)
			removedFiles = append(removedFiles, files...)
		} else {
			modTime := getFileModTime(nfsfullpath)
			// log.Println("Checking local cache file.", fullpath, modTime, fi.ModTime())

			if modTime.After(fi.ModTime()) {
				log.Println("Removing file from local cache as NFS source modified.", fullpath)
				err := os.Remove(fullpath)
				if err != nil {
					log.Println("Error deleting local cache file.", fullpath, err.Error())
				}
				removedFiles = append(removedFiles, fullpath)
			}
		}
	}
	return
}

func writeFileToFilesystem(filepath string, node *Node) {

	newfile := path.Join(cachePath, filepath)

	log.Println("Write file to FS cache.", newfile)

	// create cache dir if doesn't already exist
	err := os.MkdirAll(path.Dir(newfile), 0700)
	if err != nil {
		log.Println("Error writing file to local cache. Path:", newfile, err.Error())
	}

	// open output file
	fout, err := os.Create(newfile)
	if err != nil {
		log.Println("Error creating local cache file.", err)
		return
	}

	defer func() {
		if err = fout.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	w := bufio.NewWriter(fout)
	if _, err := w.Write(node.data); err != nil {
		log.Println("Error writing to local cache file.", err)
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
	defer file.Close()

	_, err = file.Seek(offset, 0)
	if err != nil {
		log.Println("Error seeking on local cache file.", filepath, offset, err)
		return 0
	}

	reader := bufio.NewReader(file)
	numBytes, err = reader.Read(buff)
	if err != nil {
		log.Println("Error reading from local cache file.", filepath, offset, err)
		return 0
	}
	if numBytes > 0 && (offset == 0 || endoffset == node.stat.Size) {
		log.Println("Read() - local FS cache hit:", offset, endoffset, len(buff), filepath)
	}

	return numBytes
}
