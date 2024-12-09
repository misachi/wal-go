package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type WALSTATE_t uint8

var (
	CurrentWAL *WAL
	WALGLock   atomic.Bool // CurrentWAL is set?
)

const (
	WAL_START     WALSTATE_t = 'S'
	WAL_INSERT    WALSTATE_t = 'I'
	WAL_COMMITTED WALSTATE_t = 'C'
	WAL_ABORTED   WALSTATE_t = 'A'
)

const (
	WAL_SWITCH_THRESHOLD = 0.8
	WAL_FILE_MODE        = 0600
	WALDir               = "./.tmp/wal_dir"
	WAL_MAX_SHM          = 1024 // bytes
	WAL_MAX_FILESIZE     = 500  // bytes
)

var (
	RegisterMap               = make(map[uint64]uint8)
	Logger                    = slog.New(slog.NewTextHandler(os.Stdout, nil))
	MapLock     *sync.RWMutex = &sync.RWMutex{}
)

type WALSegment struct {
	size     uint64
	file     *os.File
	location string
}

type WALEntry struct {
	size  uint32
	state WALSTATE_t
	Id    uint64
	Data  []byte
}

func (e *WALEntry) fixedSize() int {
	return int(unsafe.Sizeof(e.Id) + unsafe.Sizeof(e.size) + unsafe.Sizeof(e.state))
}

func (e *WALEntry) esize() int {
	return e.fixedSize() + len(e.Data)
}

func (e *WALEntry) toBytes() []byte {
	size := e.esize()
	bufStart := size - len(e.Data)
	buf := make([]byte, size)
	copy(buf, unsafe.Slice((*byte)(unsafe.Pointer(e)), size))
	buf2 := buf[bufStart : bufStart+len(e.Data)]  // Data area slice
	copy(buf2, e.Data)
	return buf
}

func NewWALEntry(Id uint64, data []byte) *WALEntry {
	entry := &WALEntry{
		Id: Id, Data: data,
	}
	entry.size = uint32(entry.esize())
	return entry
}

type WALHdr struct {
	segNo      atomic.Uint32
	nextSegNo  atomic.Uint32
	size       uint64
	lastSyncAt time.Time
}

func (hdr *WALHdr) writeHdr() error {
	newHdr := struct {
		size       uint64
		segno      uint32
		nextSegNo  uint32
		lastSyncAt string
	}{
		size:       hdr.size,
		segno:      hdr.segNo.Load(),
		nextSegNo:  hdr.nextSegNo.Load(),
		lastSyncAt: time.Now().Format(time.RFC3339),
	}

	metaFile, err := os.OpenFile(path.Join(WALDir, "meta"), os.O_WRONLY, WAL_FILE_MODE)
	if err != nil {
		return fmt.Errorf("writeHdr unable to open metadata file: %v", err)
	}
	defer metaFile.Close()

	size := (unsafe.Sizeof(uint32(1)) * 2) + unsafe.Sizeof(uint64(1)) + uintptr(len(newHdr.lastSyncAt))
	ptr := unsafe.Pointer(&newHdr)
	buf := unsafe.Slice((*byte)(ptr), size)
	metaFile.Write(buf)
	metaFile.Sync()
	return nil
}

func (hdr *WALHdr) readHdr() error {
	type newHdr struct {
		size       uint64
		segno      uint32
		nextSegNo  uint32
		lastSyncAt string
	}

	size := (unsafe.Sizeof(uint32(1)) * 2) + unsafe.Sizeof(uint64(1)) + uintptr(len(time.Now().Format(time.RFC3339)))

	metaFile, err := os.OpenFile(path.Join(WALDir, "meta"), os.O_RDONLY, WAL_FILE_MODE)
	if err != nil {
		return fmt.Errorf("readHdr unable to open metadata file: %v", err)
	}
	defer metaFile.Close()

	stat, err := metaFile.Stat()
	if err != nil {
		return fmt.Errorf("readHdr stat error: %v", err)
	}

	if stat.Size() > 0 {
		buf := make([]byte, size)
		_, err := metaFile.Read(buf)
		if err != nil {
			return fmt.Errorf("readHdr meta file read error: %v", err)
		}
		hdr2 := (*newHdr)(unsafe.Pointer(&buf[0]))
		if len(hdr2.lastSyncAt) > 0 {
			hdr.lastSyncAt, err = time.Parse(time.RFC3339, hdr2.lastSyncAt)
			if err != nil {
				return fmt.Errorf("readHdr date parsing error: %v", err)
			}
		}
		hdr.segNo.Store(hdr2.segno)
		hdr.nextSegNo.Store(hdr2.nextSegNo)
		hdr.size = hdr2.size
		return nil
	}
	return fmt.Errorf("readHdr meta file empty")
}

type WAL struct {
	hdr      *WALHdr
	offset   atomic.Uint64
	pinCount atomic.Uint32
	segment  *WALSegment
	mtx      *sync.Mutex
	data     []byte
	metaFile *os.File
}

func newWALSegment(size uint64, dir string) (*WALSegment, error) {
	var segment *WALSegment
	var hdr WALHdr
	var segno uint32

	Logger.Info("New log segment file")
	segment = new(WALSegment)
	err := hdr.readHdr()
	if err != nil {
		Logger.Warn("newWALSegment", "wal_header", err)
		segno = 3
	} else {
		segno = hdr.segNo.Load()
	}
	file, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%08d", segno)),
		os.O_CREATE|os.O_WRONLY, WAL_FILE_MODE)
	if err != nil {
		return nil, fmt.Errorf("newWALSegment failed to create file: %v", err)
	}
	segment.file = file
	segment.size = size
	segment.location = dir

	return segment, nil
}

func GetWAL() *WAL {
	hdr := new(WALHdr)
	segment := new(WALSegment)

	if CurrentWAL != nil {
		return CurrentWAL
	}

retry:
	if WALGLock.CompareAndSwap(false, true) {
		err := hdr.readHdr()
		if err != nil {
			Logger.Error("GetWAL header: %v", err)
			return nil
		}

		p := path.Join(WALDir, fmt.Sprintf("%08d", hdr.segNo.Load()))
		file, err := os.OpenFile(p, os.O_WRONLY, WAL_FILE_MODE)
		if err != nil {
			Logger.Error("GetWAL failed to open file", "file_open", err)
		}
		segment.file = file
		segment.size = WAL_MAX_FILESIZE
		segment.location = WALDir

		metaFile, err := os.OpenFile(path.Join(WALDir, "meta"), os.O_WRONLY, WAL_FILE_MODE)
		if err != nil {
			Logger.Error("GetWAL failed to open meta file", "file_open", err)
			return nil
		}

		CurrentWAL = &WAL{
			segment:  segment,
			mtx:      &sync.Mutex{},
			data:     make([]byte, WAL_MAX_SHM),
			metaFile: metaFile,
			hdr:      hdr,
		}
	}

	if CurrentWAL == nil {
		goto retry
	}
	return CurrentWAL

}

func NewWAL() (*WAL, error) {
	if CurrentWAL != nil {
		return CurrentWAL, nil
	}

retry:
	if WALGLock.CompareAndSwap(false, true) {
		hdr := new(WALHdr)
		err := os.MkdirAll(WALDir, WAL_FILE_MODE)
		if err != nil {
			return nil, fmt.Errorf("NewWAL new directory: %v", err)
		}

		metaFile, err := os.OpenFile(path.Join(WALDir, "meta"), os.O_CREATE|os.O_WRONLY, WAL_FILE_MODE)
		if err != nil {
			return nil, fmt.Errorf("NewWAL failed to create meta file: %v", err)
		}

		err = hdr.readHdr()
		if err != nil {
			if hdr.segNo.Load() < 1 {
				hdr.size = WAL_MAX_SHM
				hdr.segNo.Store(1)
				hdr.writeHdr()
			} else {
				return nil, fmt.Errorf("NewWAL: %v", err)
			}
		}

		segment, err := newWALSegment(WAL_MAX_FILESIZE, WALDir)
		if err != nil {
			return nil, fmt.Errorf("NewWAL unable to create wal segment: %v", err)
		}

		CurrentWAL = &WAL{
			segment:  segment,
			mtx:      &sync.Mutex{},
			data:     make([]byte, WAL_MAX_SHM),
			metaFile: metaFile,
			hdr:      hdr,
		}
	}

	if CurrentWAL == nil {
		goto retry
	}
	return CurrentWAL, nil
}

func (wal *WAL) Register(Id uint64) error {
	MapLock.RLock()
	_, ok := RegisterMap[Id]
	MapLock.RUnlock()

	if ok {
		return fmt.Errorf("Register: Data has been registered")
	}

	MapLock.Lock()
	RegisterMap[Id] = 1
	MapLock.Unlock()

	entry := WALEntry{state: WAL_START, Id: Id}
	entry.size = uint32(entry.esize())
	ret := wal.insert(&entry)
	if ret < 0 {
		wal.Unpin()
		return fmt.Errorf("Register: %v", ret)
	}
	return nil
}

func (wal *WAL) sync(syncData bool) error {
	err := wal.segment.file.Sync()
	if err != nil {
		return err
	}
	wal.resetOffset()
	wal.hdr.lastSyncAt = time.Now()
	return nil
}

func (wal *WAL) Pin() {
	cnt := wal.pinCount.Load()
	cnt += 1
	wal.pinCount.Store(cnt)
}

func (wal *WAL) Unpin() {
	cnt := wal.pinCount.Load()
	cnt -= 1
	wal.pinCount.Store(cnt)
}

func (wal *WAL) setOffset(size uint64) uint64 {
	var offset uint64
	successful := false
	for !successful {
		offset = wal.offset.Load()
		newOffset := offset + size
		successful = wal.offset.CompareAndSwap(offset, newOffset)
	}
	return offset
}

func (wal *WAL) resetOffset() {
	successful := false
	for !successful {
		offset := wal.offset.Load()
		successful = wal.offset.CompareAndSwap(offset, 0)
	}
}

func (wal *WAL) insert(entry *WALEntry) int32 {
	MapLock.RLock()
	_, ok := RegisterMap[entry.Id]
	MapLock.RUnlock()

	if !ok {
		Logger.Error("Insert: Data has not been registered")
		return -1
	}

	size := entry.esize()
	fileSize := wal.fileSize()
	if fileSize == -1 {
		Logger.Error("Insert: stat error")
		return -1
	}

	// Not enough space. We need to empty buffer
	if (wal.offset.Load() + uint64(size)) > wal.hdr.size {
		wal.writeWAL()
		wal.sync(true)
	}

	if uint64(fileSize) > uint64(WAL_SWITCH_THRESHOLD*float64(wal.segment.size)) {
		wal.extendWAL()
	}

	if (uint64(fileSize)+uint64(size)) >= wal.segment.size {
		wal.switchWAL()
	}

	// Reserve space for entry
	offset := wal.setOffset(uint64(size))

	wal.data = append(wal.data[:offset], entry.toBytes()...)
	return 0
}

func (wal *WAL) Insert(entry *WALEntry) error {
	entry.state = WAL_INSERT
	ret := wal.insert(entry)
	if ret < 0 {
		wal.Unpin()
		return fmt.Errorf("Insert: %v", ret)
	}
	return nil
}

func (wal *WAL) writeWAL() error {
	size := wal.offset.Load()
	buf := wal.data[:size]
	if len(buf) <= 0 {
		return nil
	}

	offset := wal.fileSize()
	if offset == -1 {
		offset = 0
	}
	wal.segment.file.Seek(offset, io.SeekStart)
	nr, err := wal.segment.file.Write(buf)

	if err != nil {
		return fmt.Errorf("writeWAL failed: %v", err)
	}

	if size != uint64(nr) {
		Logger.Warn("writeWAL data mismatch", "written", nr, "wanted", size)
	}
	return nil
}

func (wal *WAL) Commit(Id uint64) error {
	wal.Pin()
	defer wal.Unpin()

	entry := WALEntry{state: WAL_COMMITTED, Id: Id}
	entry.size = uint32(entry.esize())
	ret := wal.insert(&entry)
	if ret < 0 {
		wal.Unpin()
		return fmt.Errorf("Commit: %v", ret)
	}

	wal.writeWAL()
	wal.sync(false)
	return nil
}

func (wal *WAL) Abort(Id uint64) error {
	wal.Pin()
	defer wal.Unpin()
	entry := WALEntry{state: WAL_ABORTED, Id: Id}
	entry.size = uint32(entry.esize())
	ret := wal.insert(&entry)
	if ret < 0 {
		wal.Unpin()
		return fmt.Errorf("Abort: %v", ret)
	}
	wal.writeWAL()
	wal.sync(false)
	return nil
}

func (wal *WAL) fileSize() int64 {
	stat, err := wal.segment.file.Stat()
	if err != nil {
		Logger.Error("fileSize stat", "error", err)
		return -1
	}
	return stat.Size()
}

func (wal *WAL) extendWAL() {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()

	nextSegNo := wal.hdr.nextSegNo.Load()
	if nextSegNo > wal.hdr.segNo.Load() {
		Logger.Info("ExtendWAL: Next WAL segmnent file already created")
		return
	}
	newNextSegNo := nextSegNo + 1

	fileN := path.Join(wal.segment.location, fmt.Sprintf("%08d", newNextSegNo))
	file, err := os.OpenFile(fileN, os.O_CREATE, WAL_FILE_MODE)
	if err == nil {
		defer file.Close()
		wal.hdr.nextSegNo.CompareAndSwap(nextSegNo, newNextSegNo)
		wal.hdr.writeHdr()
	}
}

func (wal *WAL) switchWAL() int {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()

	nextSegNo := wal.hdr.nextSegNo.Load()

	if wal.pinCount.Load() > 1 {
		Logger.Warn("SwitchWAL: Too many pins")
		return -1
	}

	wal.segment.file.Close()
	fileN := path.Join(wal.segment.location, fmt.Sprintf("%08d", nextSegNo))
	dataFile, err := os.OpenFile(fileN, os.O_WRONLY, WAL_FILE_MODE)
	if err == nil {
		Logger.Info("Switching log file", "new_file_name", dataFile.Name())
		wal.segment.file = dataFile
		wal.hdr.segNo.Store(nextSegNo)
		wal.hdr.writeHdr()
	}
	return 0
}

func (w *WAL) Close() {
	Logger.Info("Closing log")
	CurrentWAL = nil
	WALGLock.CompareAndSwap(true, false)
	w.segment.file.Close()
	w.metaFile.Close()
}

func DumpWal(fileName string) {
	chunk := 1024
	buf := make([]byte, chunk)
	walFile, err := os.OpenFile(fileName, os.O_RDONLY, WAL_FILE_MODE)
	if err != nil {
		fmt.Printf("Open error: %v\n", err)
	}
	stat, _ := walFile.Stat()
	fileSize := stat.Size()

	for i := 0; i < int(fileSize); i += chunk {
		walFile.Seek(int64(i), io.SeekStart)
		nRead, err := walFile.Read(buf)
		if err != nil {
			fmt.Printf("Read: %v\n", err)
			break
		}

		off := 0
		for off < nRead {
			sz := *(*uint32)(unsafe.Pointer(&buf[off]))
			if (off + int(sz)) > chunk {
				break
			}
			buf2 := make([]byte, sz)
			copy(buf2, buf[off:off+int(sz)])
			entry := (*WALEntry)(unsafe.Pointer(&buf2[0]))

			arr := buf2[entry.fixedSize():]
			id := *(*byte)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&buf2[0])) + unsafe.Offsetof(entry.Id)))
			fmt.Printf("id=%d, size=%d, state=%q, data=%s\n", id, entry.size, entry.state, arr)
			off += int(sz)
		}
	}
}
