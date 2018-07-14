package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"
	"time"
)

var fusecmd *exec.Cmd

func main() {

	// create temp directory and add some data
	// ex, err := os.Executable()
	// if err != nil {
	// 	panic(err)
	// }
	// testdir := filepath.Dir(ex)
	mountpath := "C:\\temp"
	path := mountpath + "\\testdir"
	fmt.Println("Path:", path)

	createTestData(path)

	// start WinFSP
	startWinFSP(path)

	// start fusenfs
	go runfuseNFS(mountpath)

	// Wait a few seconds to mount FS
	time.Sleep(time.Second * 3)

	// verify file data
	checkFileData(path)

	// // kill fuse process
	// err := fusecmd.Process.Kill()
	// if err != nil {
	// 	panic(err)
	// }
}

// func buildExe()
// {
// 	cmd := exec.Command("go build", "-nfs-target="+target)
// 	err := cmd.Run()
// 	if err != nil {
// 		panic(err)
// 	}
// }

func createTestData(path string) {
	createDir(path)
	createTestFile(path + "\\1.file")
	createTestFile(path + "\\2.file")
}

func createDir(path string) {
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		panic(err)
	}
}
func createTestFile(path string) {
	fileHandle, err := os.Create(path)
	if err != nil {
		panic(err)
	}

	defer fileHandle.Close()

	writer := bufio.NewWriter(fileHandle)
	fmt.Fprintln(writer, "Test data")
	writer.Flush()
}

func startWinFSP(path string) {
	cmd := exec.Command("winnfsd.exe", path)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func runfuseNFS(path string) {
	path = strings.Replace(path, ":", "", 1)
	target := "/" + strings.Replace(path, "\\", "/", -1)
	fmt.Println("TargetPath:", target)

	fusecmd = exec.Command("fusenfs.exe", "-nfs-target="+target)

	// var out bytes.Buffer
	// multi := io.MultiWriter(os.Stdout, &out)
	fusecmd.Stdout = os.Stdout
	fusecmd.Stderr = os.Stderr

	err := fusecmd.Run()
	if err != nil {
		if strings.Index(err.Error(), "file does not exist") > 0 || strings.Index(err.Error(), "file not found") > 0 {
			fmt.Println("Run 'go install' in fusenfs directory before running integration tests. fusenfs.exe must end up in PATH (normally from $GOPATH\\bin)")
		}
		panic(err)
	}
}

func checkFileData(path string) {
	b, err := ioutil.ReadFile(path + "\\1.file")
	if err != nil {
		panic(err)
	}

	text := string(b)
	if strings.TrimSpace(text) != "Test data" {
		fusecmd.Process.Kill()
		fmt.Println("Test file data:", text, len(text))
		panic("Data read from test file doesn't match expected. " + string(len(text)))
	} else {
		fmt.Println("Test file verified")
	}
}
