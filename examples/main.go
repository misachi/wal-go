package main

import (
	"fmt"
	"os"
	_wal "github.com/misachi/wal-go/wal"
)

func main() {
	var id uint64 = 19

	// wal, err := NewWAL() // Without optional params

	wal, err := _wal.NewWAL(
		_wal.WithSwitchThresh(0.8),
		_wal.WithMaxSHM(1024),
		_wal.WithMaxFileSize(1024),
		_wal.WithAllowFallocate(true))

	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer wal.Close()

	entry := _wal.NewWALEntry(id, []byte("Hello World"))

	err = wal.Register(id)
	if err != nil {
		fmt.Printf("Register: %v\n", err)
		os.Exit(1)
	}

	wal.Insert(entry)
	err = wal.Commit(id, true)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	id = 20
	entry = _wal.NewWALEntry(id, []byte{87, 101, 108, 99, 111, 109, 101, 32, 116, 111, 32, 116, 104, 101, 32, 78, 101, 119, 32, 87, 111, 114, 108, 100}) // bytes: Welcome to the New World

	wal.Register(id)
	wal.Insert(entry)
	err = wal.Commit(id, true)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	id = 21
	entry = _wal.NewWALEntry(id, []byte("Make yourself at home"))

	wal.Register(id)
	wal.Insert(entry)
	err = wal.Commit(id, true)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	_wal.DumpWal(wal.SegmentFile().Name())
}
