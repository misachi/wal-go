package main

type config struct {
	wal_allow_fallocate  bool
	wal_file_mode        int32
	wal_switch_threshold float32
	wal_max_shm          uint64
	wal_max_file_size    uint64
	wal_directory        string
}

type walOption func(*config)

func WithAllowFallocate(allow bool) walOption {
	return func(c *config) {
		c.wal_allow_fallocate = allow
	}
}

func WithMaxSHM(size uint64) walOption {
	return func(c *config) {
		c.wal_max_shm = size
	}
}

func WithFileMode(mode int32) walOption {
	return func(c *config) {
		c.wal_file_mode = mode
	}
}

func WithSwitchThresh(thresh float32) walOption {
	return func(c *config) {
		c.wal_switch_threshold = thresh
	}
}

func WithMaxFileSize(size uint64) walOption {
	return func(c *config) {
		c.wal_max_file_size = size
	}
}

func WithBaseDir(location string) walOption {
	return func(c *config) {
		c.wal_directory = location
	}
}
