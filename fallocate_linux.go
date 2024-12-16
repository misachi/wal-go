package main

import "syscall"

func fallocate(fd int, offset, size int64) {
	err := syscall.Fallocate(fd, 0, offset, size)
	if err != nil {
		Logger.Warn("File allocation failed", "error_msg", err)
	}
}
