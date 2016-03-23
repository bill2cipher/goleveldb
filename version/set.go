package version

import (
  "github.com/jellybean4/goleveldb/mem"
  "github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/compact"
  "github.com/jellybean4/goleveldb/table"
)

// Manipulate a set of version
type VersionSet interface {
 
  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  LogAndApply(edit VersionEdit) error
  
  // Recover the last saved descriptor from persistent storage
  Recover() error
  
  // return the current version within the version set
  Current() Version
  
  // Return the current manifest file number
  ManifestFileNumber() int
  
  // Allocate and return a new file number
  NewFileNumber() int
  
  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  ReuseFileNumber(num int) error
  
  // Return the number of TAble files at the specified level
  NumLevelFiles(level int) int
  
  // Return the combined file size of all files at the specified level
  NumLevelBytes(level int) int
  
  // Set the last sequence number to seq
  SetLastSequence(seq uint64) error  
  
  // Return the last sequence number within the version set
  LastSequence() uint64
  
  // Mark the specified file number as used
  MarkFileNumberUsed(num int)
  
  // Return the current log file number
  LogNumber() int
  
  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  PrevLogNumber() int
  
  // Pick level and inputs for a new compaction.
  // Returns NULL if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  PickCompaction() compact.Compaction
  
  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  MaxNextLevelOverlappingBytes() int
  
  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  MakeInputIterator(c compact.Compaction) mem.Iterator
  
  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns NULL if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  CompactRange(level int, begin, end *util.InternalKey)
  
  // Returns true iff some level needs a compaction.
  NeedsCompactoin() bool
 
  // Get all files listed in any live version
  // May also mutate some internal state.
  GetLIveFiles() []int
  
  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  ApproximateOffsetOf(v Version, key *util.InternalKey) int
  
  // Return the name of db
  DBName() string
  
  // Table cache of the db
  TableCache() table.TableCache
  
  // options set with the db  
  Option() *util.Option
  
  GetRange()
  
  GetRange2()
  
  SetupOtherInputs()
  
  AppendVersion(v Version)
}
