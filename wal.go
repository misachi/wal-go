package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
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
	// 	WAL_SWITCH_THRESHOLD = 0.8
	META_FILE_MODE = 0600

// WALDir               = "./.tmp/wal_dir"
// WAL_MAX_SHM          = 1024 // bytes
// WAL_MAX_FILESIZE     = 500  // bytes
// WAL_FALLOCATE        = true
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

func (e *WALEntry) fixedSize() int {
	sz := int(unsafe.Sizeof(e.id) + unsafe.Sizeof(e.size) + unsafe.Sizeof(e.state))
	return ((sz + 7) & ^7) // 8 byte alignment
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
	size       uint64 // Keep track of size of current segment file
	lastSyncAt time.Time
	mtx        *sync.RWMutex
}

func (hdr *WALHdr) writeHdr(metaFileDir string) error {
	hdr.mtx.Lock()
	defer hdr.mtx.Unlock()

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

	metaFile, err := os.OpenFile(path.Join(metaFileDir, "meta"), os.O_WRONLY, META_FILE_MODE)
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

func (hdr *WALHdr) readHdr(metaFileDir string) error {
	hdr.mtx.RLock()
	defer hdr.mtx.RUnlock()

	type newHdr struct {
		Size       uint64
		Segno      uint32
		NextSegNo  uint32
		LastSyncAt string
	}

	metaFile, err := os.OpenFile(path.Join(metaFileDir, "meta"), os.O_RDONLY, META_FILE_MODE)
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
	hdr       *WALHdr
	segment   *WALSegment
	mtx       *sync.Mutex
	metaFile  *os.File
	cfg       *config
	offset    atomic.Uint64
	lastWrite uint64
	data      []byte
}

func newWALSegment(size uint64, dir string) (*WALSegment, error) {
	var segment *WALSegment
	var hdr *WALHdr
	var segno uint32

	Logger.Info("New log segment file")
	segment = new(WALSegment)

	hdr = CurrentWAL.hdr
	err := hdr.readHdr(dir)
	if err != nil {
		return nil, fmt.Errorf("newWALSegment WAL header error: %v", err)
	} else {
		segno = hdr.segNo.Load()
	}
	file, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%08d", segno)),
		os.O_CREATE|os.O_WRONLY, os.FileMode(CurrentWAL.cfg.wal_file_mode))
	if err != nil {
		return nil, fmt.Errorf("newWALSegment failed to create file: %v", err)
	}
	segment.file = file
	segment.size = size
	segment.location = dir

	return segment, nil
}

func NewWAL(cfgOptions ...walOption) (*WAL, error) {
	if CurrentWAL != nil {
		return CurrentWAL, nil
	}

	cfg := &config{}
	for _, opt := range cfgOptions {
		opt(cfg)
	}

	if cfg.wal_directory == "" {
		cfg.wal_directory = "./.tmp/wal_dir"
	}

	if cfg.wal_file_mode == 0 {
		cfg.wal_file_mode = 0600
	}

	if cfg.wal_switch_threshold == 0 {
		cfg.wal_switch_threshold = 0.7
	}

	if cfg.wal_max_shm <= 0 {
		cfg.wal_max_shm = 1 << 20 // 1MB
	}

	if cfg.wal_max_file_size <= 0 {
		cfg.wal_max_file_size = 1 << 20 // 1 MB
	}

retry:
	if WALGLock.CompareAndSwap(false, true) {
		hdr := new(WALHdr)
		err := os.MkdirAll(cfg.wal_directory, os.FileMode(cfg.wal_file_mode))
		if err != nil {
			return nil, fmt.Errorf("NewWAL new directory: %v", err)
		}

		metaPath := path.Join(cfg.wal_directory, "meta")

		metaFile, err := os.OpenFile(metaPath, os.O_CREATE|os.O_WRONLY, os.FileMode(cfg.wal_file_mode))
		if err != nil {
			return nil, fmt.Errorf("NewWAL failed to open meta file: %v", err)
		}

		hdr.mtx = &sync.RWMutex{}

		err = hdr.readHdr(cfg.wal_directory)
		if err != nil {
			if hdr.segNo.Load() < 1 {
				hdr.lastSyncAt = time.Now()
				hdr.size = 0
				hdr.segNo.Store(1)
				hdr.nextSegNo.Store(1)
				hdr.writeHdr(cfg.wal_directory)
			} else {
				return nil, fmt.Errorf("NewWAL: %v", err)
			}
		}

		CurrentWAL = &WAL{
			mtx:       &sync.Mutex{},
			data:      make([]byte, cfg.wal_max_shm),
			metaFile:  metaFile,
			hdr:       hdr,
			cfg:       cfg,
			lastWrite: 0,
		}

		// CurrentWAL.offset.Store(hdr.size)

		segment, err := newWALSegment(cfg.wal_max_file_size, cfg.wal_directory)
		if err != nil {
			return nil, fmt.Errorf("NewWAL unable to create wal segment: %v", err)
		}

		CurrentWAL.segment = segment

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

func (wal *WAL) sync(offset, size uint64) error {
	err := wal.segment.file.Sync()
	if err != nil {
		return err
	}

	wal.hdr.mtx.Lock()
	wal.hdr.lastSyncAt = time.Now()
	wal.lastWrite = wal.offset.Load()
	wal.hdr.mtx.Unlock()

	wal.hdr.writeHdr(wal.cfg.wal_directory)
	return nil
}

func (wal *WAL) setOffset(size uint64) uint64 {
	var offset uint64
	successful := false
	for !successful {
		offset = wal.offset.Load()
		newOffset := offset + size
		if newOffset >= wal.cfg.wal_max_shm {
			newOffset = 0
		}
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

	// Not enough space. We need to empty buffer
	if (wal.offset.Load() + uint64(size)) > wal.cfg.wal_max_shm {
		wal.writeWAL()
		wal.sync(0, wal.offset.Load())
	}

	if (wal.hdr.size + uint64(size)) > uint64(wal.cfg.wal_switch_threshold*float32(wal.segment.size)) {
		wal.extendWAL()
	}

	if (wal.hdr.size + uint64(size)) >= wal.segment.size {
		wal.switchWAL()
	}

	// Reserve space for entry
	offset := wal.setOffset(uint64(size))

	copy(wal.data[offset:offset+uint64(size)], entry.toBytes())

	return nil
}

func (wal *WAL) Insert(entry *WALEntry) error {
	entry.state = WAL_INSERT
	return wal.insert(entry)
}

// writeWAL writes the data to fs buffers. This does not guarantee the data
// has been persisted to permanent storage. `WAL.sync` must be called for this purpose
func (wal *WAL) writeWAL() error {
	size := wal.offset.Load()

	var buf []byte
	if size > wal.lastWrite {
		buf = wal.data[wal.lastWrite:size]
	} else {
		buf = wal.data[wal.lastWrite:]
	}

	if len(buf) <= 0 {
		return nil
	}

	// make sure we have the right offset to write to
	if wal.hdr.size <= 0 {
		wal.segment.file.Seek(0, io.SeekStart)
	} else {
		wal.segment.file.Seek(int64(wal.hdr.size), io.SeekStart)
	}
	nr, err := wal.segment.file.Write(buf)

	wal.hdr.mtx.Lock()
	wal.hdr.size += uint64(len(buf))
	wal.hdr.mtx.Unlock()

	if err != nil {
		return fmt.Errorf("writeWAL failed: %v", err)
	}

	if len(buf) != nr {
		Logger.Warn("writeWAL data mismatch", "written", nr, "wanted", len(buf))
	}

	return nil
}

func (wal *WAL) Commit(Id uint64, doWait bool) error {
	entry := WALEntry{state: WAL_COMMITTED, id: Id}
	entry.size = uint32(entry.esize())
	err := wal.insert(&entry)
	if err != nil {
		return fmt.Errorf("Commit: %v", err)
	}

	if doWait {
		wal.writeWAL()
		wal.sync(0, wal.offset.Load())
	}
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
	wal.sync(0, wal.offset.Load())
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
		file, err := os.OpenFile(fileN, os.O_CREATE|os.O_WRONLY, os.FileMode(wal.cfg.wal_file_mode))
		if err == nil {
			defer file.Close()
			Logger.Info("Creating new log file", "new_file_name", file.Name())
			wal.hdr.writeHdr(wal.cfg.wal_directory)
			if wal.cfg.wal_allow_fallocate {
				fallocate(int(file.Fd()), 0, int64(wal.segment.size))
			}
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
	dataFile, err := os.OpenFile(fileN, os.O_WRONLY, os.FileMode(wal.cfg.wal_file_mode))
	if err == nil {
		Logger.Info("Switching log file", "new_file_name", dataFile.Name())
		wal.segment.file = dataFile
		wal.hdr.segNo.Store(nextSegNo)
		wal.hdr.writeHdr(wal.cfg.wal_directory)
		wal.segment.file.Seek(0, io.SeekStart)
		wal.hdr.size = 0
		return
	}
	Logger.Error("switchWAL error", "err_msg", err)
}

func (w *WAL) Close() {
	Logger.Info("Closing log")
	CurrentWAL = nil
	WALGLock.CompareAndSwap(true, false)
	w.segment.file.Close()
	w.metaFile.Close()
}

func (w *WAL) BGWriter(ctx context.Context, interval int32) {
	ticker := time.NewTicker(time.Duration(interval) * time.Microsecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			off := w.offset.Load()
			if off > 0 {
				w.writeWAL()
				w.sync(0, w.offset.Load())
			}
		}
	}
}

func DumpWal(fileName string) {
	chunk := 4096
	buf := make([]byte, chunk)
	walFile, err := os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		fmt.Printf("Open error: %v\n", err)
	}

	dir := filepath.Dir(fileName)

	hdr := new(WALHdr)
	hdr.mtx = &sync.RWMutex{}
	err = hdr.readHdr(dir)
	if err != nil {
		fmt.Printf("DumpWal: Meta file read error: %v\n", err)
	}

	stat, _ := walFile.Stat()

	var i int64
	for i = 0; i < stat.Size(); i += int64(chunk) {
		walFile.Seek(int64(i), io.SeekStart)
		nRead, err := walFile.Read(buf)
		if err != nil {
			fmt.Printf("Read: %v\n", err)
			break
		}

		off := 0
		for off < nRead {
			var state string

			entry := *(*WALEntry)(unsafe.Pointer(&buf[off]))
			sz := entry.size
			if sz == 0 {
				break
			}

			// TODO: Handle records that span multiple pages. We are currently skipping such records
			if (off + int(sz)) > chunk {
				off += int(sz)
				break
			}
			arr := buf[off+entry.fixedSize() : off+int(sz)]

			switch entry.state {
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
			fmt.Printf("id=%d, size=%d, state=%s, data=%s\n", entry.id, sz, state, arr)
			off += int(sz)
		}
	}
}
