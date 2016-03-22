package util

import (
  "github.com/jellybean4/goleveldb/filter"
)

func init() {
  DefaultOption.Interval = 16
  DefaultOption.BlockSize = 4096
  DefaultOption.Policy = filter.NewBloomPolicy(10)
  DefaultOption.Comparator = BinaryComparator
  DefaultOption.BufferSize = 4096
}

type Option struct {
  Interval   int
  BlockSize  int
  Policy     filter.Policy
  Comparator Comparator
  BufferSize int
}

var DefaultOption Option


func init() {
  DefaultReadOption.Verify = true
  DefaultReadOption.Cache  = false
}
type ReadOption struct {
  Verify bool
  Cache  bool
}

var DefaultReadOption ReadOption

// Options that control write operations
type WriteOption struct {
  
  // If true, the write will be flushed from the operating system
  // buffer cache (by calling WritableFile::Sync()) before the write
  // is considered complete.  If this flag is true, writes will be
  // slower.
  //
  // If this flag is false, and the machine crashes, some recent
  // writes may be lost.  Note that if it is just the process that
  // crashes (i.e., the machine does not reboot), no writes will be
  // lost even if sync==false.
  //
  // In other words, a DB write with sync==false has similar
  // crash semantics as the "write()" system call.  A DB write
  // with sync==true has similar crash semantics to a "write()"
  // system call followed by "fsync()".
  Sync bool
}

func init() {
  DefaultWriteOption.Sync = false
}
var DefaultWriteOption WriteOption