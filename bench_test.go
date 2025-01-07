package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func dummy_write(wal *WAL) {
	var id uint64 = rand.Uint64()

	entry := NewWALEntry(id, []byte("Hello World"))

	err := wal.Register(id)
	if err != nil {
		fmt.Printf("Register: %v\n", err)
		os.Exit(1)
	}

	wal.Insert(entry)
	err = wal.Commit(id, false)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	id = rand.Uint64()
	entry = NewWALEntry(id, []byte{87, 101, 108, 99, 111, 109, 101, 32, 116, 111, 32, 116, 104, 101, 32, 78, 101, 119, 32, 87, 111, 114, 108, 100}) // bytes: Welcome to the New World

	wal.Register(id)
	wal.Insert(entry)
	err = wal.Commit(id, false)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}

	id = rand.Uint64()
	entry = NewWALEntry(id, []byte("Make yourself at home"))

	wal.Register(id)
	wal.Insert(entry)
	err = wal.Commit(id, false)
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
}

func BenchmarkSingleWriter(b *testing.B) {
	wal, err := NewWAL(
		WithSwitchThresh(0.8),
		WithMaxSHM(1024*1024*128),
		WithMaxFileSize(1024*1024*1024),
		WithAllowFallocate(true))

	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer wal.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go wal.BGWriter(ctx, 5)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dummy_write(wal)
	}
}

func BenchmarkParallelWriter(b *testing.B) {
	wal, err := NewWAL(
		WithSwitchThresh(0.8),
		WithMaxSHM(1024*1024*128),
		WithMaxFileSize(1024*1024*1024),
		WithAllowFallocate(true),
		WithBaseDir(".tmp/wal_dir2"))
	if err != nil {
		fmt.Printf("%v\n", err)
		os.Exit(1)
	}
	defer wal.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go wal.BGWriter(ctx, 5)

	b.ResetTimer()

	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			dummy_write(wal)
		}
	})

}
