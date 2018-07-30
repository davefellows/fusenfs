// +build windows

package main

// getFileAttributes OS-specific implementation.
// We just return zeros on Windows
func getFileAttributes(sys interface{}) (ino uint64, uid, gui uint32) {
	return 0, 1, 1
}
