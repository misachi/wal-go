package main

import (
	"fmt"
	"os"
)

func main() {
	var id uint64 = 19

	wal, err := NewWAL()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer wal.Close()

	entry := NewWALEntry(uint64(id), []byte("Hello World"))

	err = wal.Register(uint64(id))
	if err != nil {
		fmt.Printf("Register: %v\n", err)
		os.Exit(1)
	}

	wal.Insert(entry)
	err = wal.Commit(uint64(id))
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	id = 20
	entry = NewWALEntry(uint64(id), []byte("Welcome to the New World"))

	wal.Register(uint64(id))
	wal.Insert(entry)
	err = wal.Commit(uint64(id))
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	id = 21
	entry = NewWALEntry(uint64(id), []byte("Make yourself at home"))

	wal.Register(uint64(id))
	wal.Insert(entry)
	err = wal.Commit(uint64(id))
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	DumpWal(wal.segment.file.Name())
}
