Write-Ahead-Log written in go

Example usage:
```
id := 92
wal, _ := NewWAL() // Obtain WAL handle
defer wal.Close()

entry := NewWALEntry(uint64(id), []byte("Hello World")) // Build WAL entry

wal.Register(uint64(id))  // Register your data
wal.Insert(entry)  // Data to be recorded
wal.Commit(uint64(id)) // Save data to storage

DumpWal(wal.segment.file.Name()) // List all WAL records

```