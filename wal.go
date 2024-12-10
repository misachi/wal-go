package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"syscall"
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
	id    uint64
	data  []byte
}

func fallocate(fd int, offset int64, size int64) {
	err := syscall.Fallocate(fd, 0, 0, size)
	if err != nil {
		Logger.Warn("File allocation failed", "error_msg", err)
	}
}

func (e *WALEntry) fixedSize() int {
	return int(unsafe.Sizeof(e.id) + unsafe.Sizeof(e.size) + unsafe.Sizeof(e.state))
}

func (e *WALEntry) esize() int {
	return e.fixedSize() + len(e.data)
}

func (e *WALEntry) toBytes() []byte {
	size := e.esize()
	bufStart := size - len(e.data)
	buf := make([]byte, size)
	copy(buf, unsafe.Slice((*byte)(unsafe.Pointer(e)), size))
	buf2 := buf[bufStart : bufStart+len(e.data)] // Data area slice
	copy(buf2, e.data)
	return buf
}

func NewWALEntry(Id uint64, data []byte) *WALEntry {
	entry := &WALEntry{
		id: Id, data: data,
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
	type newHdr struct {
		Size       uint64
		Segno      uint32
		NextSegNo  uint32
		LastSyncAt string
	}

	nHdr := newHdr{
		Size:       hdr.size,
		Segno:      hdr.segNo.Load(),
		NextSegNo:  hdr.nextSegNo.Load(),
		LastSyncAt: hdr.lastSyncAt.Format(time.RFC3339),
	}

	metaFile, err := os.OpenFile(path.Join(WALDir, "meta"), os.O_WRONLY, WAL_FILE_MODE)
	if err != nil {
		return fmt.Errorf("writeHdr unable to open metadata file: %v", err)
	}
	defer metaFile.Close()

	var buf bytes.Buffer

	enc := gob.NewEncoder(&buf)

	err = enc.Encode(nHdr)
	if err != nil {
		fmt.Printf("%v\n", err)
		return fmt.Errorf("writeHdr encoding error: %v", err)
	}

	nr, err := metaFile.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("writeHdr write error: %s", err)
	}

	if buf.Len() != nr {
		Logger.Warn("writeHdr data mismatch", "written", nr, "wanted", buf.Len())
	}
	return nil
}

func (hdr *WALHdr) readHdr() error {
	type newHdr struct {
		Size       uint64
		Segno      uint32
		NextSegNo  uint32
		LastSyncAt string
	}

	metaFile, err := os.OpenFile(path.Join(WALDir, "meta"), os.O_RDONLY, WAL_FILE_MODE)
	if err != nil {
		return fmt.Errorf("readHdr unable to open metadata file: %v", err)
	}
	defer metaFile.Close()

	stat, err := metaFile.Stat()
	if err != nil {
		return fmt.Errorf("readHdr stat error: %v", err)
	}
	fileSize := stat.Size()

	if fileSize > 0 {
		buf1 := make([]byte, fileSize)
		_, err := metaFile.Read(buf1)
		if err != nil {
			return fmt.Errorf("readHdr meta file read error: %v", err)
		}

		buf := bytes.NewBuffer(buf1)
		dec := gob.NewDecoder(buf)
		var nHdr newHdr

		err = dec.Decode(&nHdr)
		if err != nil {
			return fmt.Errorf("readHdr decoding error: %v", err)
		}

		hdr.lastSyncAt, err = time.Parse(time.RFC3339, nHdr.LastSyncAt)
		if err != nil {
			return fmt.Errorf("time parsing error")
		}

		hdr.segNo.Store(nHdr.Segno)
		hdr.nextSegNo.Store(nHdr.NextSegNo)
		hdr.size = nHdr.Size
		return nil
	}
	return fmt.Errorf("readHdr meta file empty")
}

type WAL struct {
	hdr      *WALHdr
	segment  *WALSegment
	mtx      *sync.Mutex
	metaFile *os.File
	offset   atomic.Uint64
	data     []byte
}

func newWALSegment(size uint64, dir string) (*WALSegment, error) {
	var segment *WALSegment
	var hdr WALHdr
	var segno uint32

	Logger.Info("New log segment file")
	segment = new(WALSegment)
	err := hdr.readHdr()
	if err != nil {
		return nil, fmt.Errorf("newWALSegment WAL header error: %v", err)
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

		metaPath := path.Join(WALDir, "meta")

		metaFile, err := os.OpenFile(metaPath, os.O_CREATE|os.O_WRONLY, WAL_FILE_MODE)
		if err != nil {
			return nil, fmt.Errorf("NewWAL failed to open meta file: %v", err)
		}

		err = hdr.readHdr()
		if err != nil {
			if hdr.segNo.Load() < 1 {
				hdr.lastSyncAt = time.Now()
				hdr.size = WAL_MAX_SHM
				hdr.segNo.Store(1)
				hdr.nextSegNo.Store(1)
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

	entry := WALEntry{state: WAL_START, id: Id}
	entry.size = uint32(entry.esize())
	err := wal.insert(&entry)
	if err != nil {
		return fmt.Errorf("Register: %v", err)
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

func (wal *WAL) insert(entry *WALEntry) error {
	MapLock.RLock()
	_, ok := RegisterMap[entry.id]
	MapLock.RUnlock()

	if !ok {
		return fmt.Errorf("Insert: Data has not been registered")
	}

	size := entry.esize()
	fileSize := wal.fileSize()
	if fileSize == -1 {
		return fmt.Errorf("Insert: stat error")
	}

	// Not enough space. We need to empty buffer
	if (wal.offset.Load() + uint64(size)) > wal.hdr.size {
		wal.writeWAL()
		wal.sync(true)
	}

	if uint64(fileSize) > uint64(WAL_SWITCH_THRESHOLD*float64(wal.segment.size)) {
		wal.extendWAL()
	}

	if (uint64(fileSize) + uint64(size)) >= wal.segment.size {
		wal.switchWAL()
	}

	// Reserve space for entry
	offset := wal.setOffset(uint64(size))

	wal.data = append(wal.data[:offset], entry.toBytes()...)
	return nil
}

func (wal *WAL) Insert(entry *WALEntry) error {
	entry.state = WAL_INSERT
	return wal.insert(entry)
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
	entry := WALEntry{state: WAL_COMMITTED, id: Id}
	entry.size = uint32(entry.esize())
	err := wal.insert(&entry)
	if err != nil {
		return fmt.Errorf("Commit: %v", err)
	}

	wal.writeWAL()
	wal.sync(false)
	return nil
}

func (wal *WAL) Abort(Id uint64) error {
	entry := WALEntry{state: WAL_ABORTED, id: Id}
	entry.size = uint32(entry.esize())
	err := wal.insert(&entry)
	if err != nil {
		return fmt.Errorf("Abort: %v", err)
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
		Logger.Info("Next WAL segment file already created")
		return
	}

	newNextSegNo := nextSegNo + 1
	wal.hdr.nextSegNo.CompareAndSwap(nextSegNo, newNextSegNo)

	go func() {
		fileN := path.Join(wal.segment.location, fmt.Sprintf("%08d", newNextSegNo))
		file, err := os.OpenFile(fileN, os.O_CREATE|os.O_WRONLY, WAL_FILE_MODE)
		if err == nil {
			defer file.Close()
			wal.hdr.writeHdr()
			fallocate(int(file.Fd()), 0, int64(wal.segment.size))
			file.Sync()
		}
	}()
}

func (wal *WAL) switchWAL() {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()

	nextSegNo := wal.hdr.nextSegNo.Load()
	if nextSegNo == wal.hdr.segNo.Load() {
		Logger.Info("Already switched log files")
		return
	}

	wal.segment.file.Close() // Closing old segment file
	fileN := path.Join(wal.segment.location, fmt.Sprintf("%08d", nextSegNo))
	dataFile, err := os.OpenFile(fileN, os.O_WRONLY, WAL_FILE_MODE)
	if err == nil {
		Logger.Info("Switching log file", "new_file_name", dataFile.Name())
		wal.segment.file = dataFile
		wal.hdr.segNo.Store(nextSegNo)
		wal.hdr.writeHdr()
	}
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
			var state string
			sz := *(*uint32)(unsafe.Pointer(&buf[off]))
			if (off + int(sz)) > chunk {
				break
			}

			entry := WALEntry{}
			arr := buf[off+entry.fixedSize():off+int(sz)]
			id := *(*byte)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&buf[off])) + unsafe.Offsetof(entry.id)))
			eState := *(*WALSTATE_t)(unsafe.Pointer(
				uintptr(unsafe.Pointer(&buf[off])) + unsafe.Offsetof(entry.state)))

			switch eState {
			case WAL_START:
				state = "START"
			case WAL_INSERT:
				state = "INSERT"
			case WAL_COMMITTED:
				state = "COMMMIT"
			case WAL_ABORTED:
				state = "ABORT"
			default:
				Logger.Warn("Unknown state")
			}
			fmt.Printf("id=%d, size=%d, state=%s, data=%s\n", id, sz, state, arr)
			off += int(sz)
		}
	}
}
