Write-Ahead-Log written in go

Example usage:
```
wal, _ := NewWAL()
defer wal.Close()

entry := NewWALEntry(uint64(id), []byte("Hello World"))

wal.Register(entry.Id)
wal.Insert(entry)
wal.Commit(entry.Id)

```