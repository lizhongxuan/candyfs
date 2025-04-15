package meta

import "time"

// Config for clients.
type Config struct {
	Strict             bool // update ctime
	Retries            int
	MaxDeletes         int
	SkipDirNlink       int
	CaseInsensi        bool
	ReadOnly           bool
	NoBGJob            bool // disable background jobs like clean-up, backup, etc.
	OpenCache          time.Duration
	OpenCacheLimit     uint64 // max number of files to cache (soft limit)
	Heartbeat          time.Duration
	MountPoint         string
	Subdir             string
	AtimeMode          string
	DirStatFlushPeriod time.Duration
	SkipDirMtime       time.Duration
	Sid                uint64
	SortDir            bool
}

func DefaultConf() *Config {
	return &Config{Strict: true, Retries: 10, MaxDeletes: 2, Heartbeat: 12 * time.Second, AtimeMode: NoAtime, DirStatFlushPeriod: 1 * time.Second}
}

func (*Config) SelfCheck() {
	return
}
