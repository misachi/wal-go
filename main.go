package main

import (
	"fmt"
	"os"
)

func main() {
	var id = 19

	wal, err := NewWAL()
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer wal.Close()

	entry := NewWALEntry(uint64(id), []byte("Hello World"))

	err = wal.Register(entry.Id)
	if err != nil {
		fmt.Printf("Register: %v\n", err)
		os.Exit(1)
	}

	wal.Insert(entry)
	err = wal.Commit(entry.Id)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	entry = NewWALEntry(20, []byte("Welcome to the New World"))

	wal.Register(entry.Id)
	wal.Insert(entry)
	err = wal.Commit(entry.Id)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	entry = NewWALEntry(21, []byte("Make yourself at home"))

	wal.Register(entry.Id)
	wal.Insert(entry)
	err = wal.Commit(entry.Id)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	DumpWal(wal.segment.file.Name())
}
