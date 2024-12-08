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

	// wal := GetWAL()
	// defer wal.Close()

	DumpWal(wal.segment.file.Name())

	// *entry = WALEntry{
	// 	Id:    19,
	// 	state: WAL_ABORTED,
	// 	Data:  []byte("Hello World"),
	// }

	// arr := make([]byte, 20)

	// slice := entry.Data[:]
	// size := int(unsafe.Sizeof(entry.Id)+unsafe.Sizeof(entry.state)) + len(entry.Data)

	// fmt.Println(*(*uint64)(unsafe.Pointer(&entry)))
	// fmt.Println(*(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&entry)) + unsafe.Offsetof(entry.state))))
	// data := (*[]byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&entry)) + unsafe.Offsetof(entry.Data)))
	// fmt.Printf("%s\n", *data)
	// fmt.Println(cap(*data))
	// fmt.Println(entry.Data, "\n", *data, "\n", slice)
	// fmt.Println(size, unsafe.Sizeof(entry))
	// val := unsafe.Pointer(&entry)
	// fmt.Printf("%v\n", *(*uint8)(val))
	// fmt.Println(strconv.Atoi("000000123"))

	// // val2 := (*int)(val)
	// for i := 0; i < size; i++ {
	// 	arr[i] = *(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&entry)) + uintptr(i)))
	// }
	// fmt.Printf("Loop: %b\n", arr)

	// arr2 := unsafe.Slice((*byte)(unsafe.Pointer(val)), size)

	// copy(arr, arr2)

	// fmt.Printf("Copy: %b\n", arr)

	// fmt.Printf("%s\n", *(*[]byte)(val))

	// copy(arr, *(*[20]byte)(val))
	// unsafe.SliceData(unsafe.Pointer(&entry))
	// arr = append(arr, *(*[]byte)(unsafe.Pointer(&entry))...)
	// fmt.Printf("%s\n", arr)

	// wal.Insert()
}
