package util

// Config defines settings for database
type Config struct {
  L0StopWritesTrigger int
  MaxLevel int
  MaxSeq uint64
}

// Global defines default db settings
var Global Config

func init() {
  Global.L0StopWritesTrigger = 1024 * 1024 * 4
  Global.MaxLevel = 7
  Global.MaxSeq = 0x1 << 56 - 1
}
