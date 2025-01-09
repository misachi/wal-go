//go:build !linux

package wal

import (
	"log"
	"os"
)

const PAGE_SIZE int = 500

func fallocate(fd int, offset, size int64) {
	buf := [PAGE_SIZE]byte{}
	file := os.NewFile(uintptr(fd), "falloc")

	if file == nil {
		log.Printf("Invalid file descriptor: %d\n", fd)
		return
	}

	defer file.Close()

	if (size - offset) < int64(PAGE_SIZE) {
		log.Printf("Allocation region should be at least %d bytes\n", PAGE_SIZE)
		return
	}

	if ((size - offset) % int64(PAGE_SIZE)) != 0 {
		log.Printf("Allocation region must be PAGE_SIZE=%d aligned\n", PAGE_SIZE)
		return
	}

	for i := offset; i < size; i += int64(PAGE_SIZE) {
		file.Write(buf[:])
	}
}
