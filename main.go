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

	entry := NewWALEntry(id, []byte("Hello World"))

	err = wal.Register(id)
	if err != nil {
		fmt.Printf("Register: %v\n", err)
		os.Exit(1)
	}

	wal.Insert(entry)
	err = wal.Commit(id)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	id = 20
	entry = NewWALEntry(id, []byte{87, 101,108, 99, 111, 109, 101, 32, 116, 111, 32, 116, 104, 101, 32, 78, 101, 119, 32, 87, 111, 114, 108, 100}) // bytes: Welcome to the New World

	wal.Register(id)
	wal.Insert(entry)
	err = wal.Commit(id)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	id = 21
	entry = NewWALEntry(id, []byte("Make yourself at home"))

	wal.Register(id)
	wal.Insert(entry)
	err = wal.Commit(id)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	DumpWal(wal.segment.file.Name())
}
