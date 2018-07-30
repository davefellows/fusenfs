// +build linux darwin

package main

import (
	"fmt"
	"syscall"
)

// getFileAttributes OS-specific implementation.
func getFileAttributes(sys interface{}) (ino uint64, uid, gui uint32) {
	stat, ok := sys.(*syscall.Stat_t)
	if !ok {
		fmt.Println("getFileAttributes() - Not a syscall.Stat_t")
		return
	}
	return stat.Ino, stat.Uid, stat.Gid
}
