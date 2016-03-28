package util

// Config defines settings for database
type Config struct {
  L0StopWritesTrigger int
  MaxLevel int
  MaxSeq uint64
  // Maximum level to which a new compacted memtable is pushed if it
  // does not create overlap.  We try to push to level 2 to avoid the
  // relatively expensive level 0=>1 compactions and to avoid some
  // expensive manifest file operations.  We do not push all the way to
  // the largest level since that can generate a lot of wasted disk
  // space if the same key space is being repeatedly overwritten.
  MaxMemCompactLevel int
  
  TargetFileSize int

  // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
  // stop building a single file in a level->level+1 compaction.
  MaxGrandParentOverlapBytes int

  // Maximum number of bytes in all compacted files.  We avoid expanding
  // the lower level file set of a compaction if it would make the
  // total compaction cover more than this many bytes.
  ExpandedCompactionByteSizeLimit int
  
  // When level0 files need to be under compaction
  L0CompactionTrigger int
  
  TableCacheEntries int
}

// Global defines default db settings
var Global Config

func init() {
  Global.L0StopWritesTrigger = 1024 * 1024 * 4
  Global.MaxLevel = 7
  Global.MaxSeq = 0x1 << 56 - 1
  Global.MaxMemCompactLevel = 2
  Global.TargetFileSize = 2 * 1048576
  Global.MaxGrandParentOverlapBytes = 10 * Global.TargetFileSize
  Global.ExpandedCompactionByteSizeLimit = 25 * Global.TargetFileSize
  Global.L0CompactionTrigger = 4
  Global.TableCacheEntries = 16
}
