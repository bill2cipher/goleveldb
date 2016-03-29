package version

import (
  "os"
  "io"
  "fmt"
  "bytes"
  "bufio"
  "errors"
)

import (
  "code.google.com/p/log4go"
)

import (
  "github.com/jellybean4/goleveldb/mem"
  "github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/compact"
  "github.com/jellybean4/goleveldb/table"
	"github.com/jellybean4/goleveldb/log"
)

// Manipulate a set of version
type VersionSet struct {
  current *Version
  option  *util.Option
  pointer []*table.FileMetaData
  cache   table.TableCache
  dbname  string
  logNum  int
  lastSeq uint64
  fileNum int
  descNum int
  writer  log.Writer
}

func NewVersionSet(db string, option *util.Option, cache table.TableCache) *VersionSet {
  set := new(VersionSet)
  if err := set.init(db, option, cache); err != nil {
    return nil
  }
  return set
}

func (set *VersionSet) init(db string, option *util.Option, cache table.TableCache) error {
  set.option = option
  set.current = NewVersion(set)
  set.cache = cache
  set.dbname = db
  set.pointer = make([]*table.FileMetaData, util.Global.MaxLevel)
  return set.Recover()
}

// Apply *edit to the current version to form a new descriptor that
// is both saved to persistent state and installed as the new
// current version.  Will release *mu while actually writing to the file.
// REQUIRES: *mu is held on entry.
// REQUIRES: no other thread concurrently calls LogAndApply()
func (set *VersionSet) LogAndApply(edit *VersionEdit) error {
  if set.current == nil {
    return errors.New("current version invalid")
  }
  
  set.setVersionEdit(edit)
  version := NewVersion(set)
  builder := NewVersionBuilder(set.current, set.option.Comparator)
  builder.Apply(edit)
  builder.Finish(version)
  
  set.append(version)
  set.current.cscore, set.current.clevel = set.scoreCompaction(set.current)
  if set.writer == nil {
    descName := util.DescriptorFileName(set.dbname, set.descNum)
    if writer, err := log.NewWriter(descName); err != nil {
      return err
    } else {
      set.writer = writer
    }
    set.writeSnapshot()
  }
  
  if err := set.writer.AddRecord(edit.Encode()); err != nil {
    return err
  }
  util.SetCurrentFile(set.dbname, set.descNum)
  log4go.Info("%s", edit.dumpInfo())
  log4go.Info("%s", set.dumpCurrent())
  return nil
}


// Recover the last saved descriptor from persistent storage
func (set *VersionSet) Recover() error {
  current := util.CurrentFileName(set.dbname)
  if _, err := os.Stat(current); err != nil && os.IsNotExist(err) {
    set.descNum = set.NewFileNumber()
    return nil
  }
  
  descName := set.parseCurrentFile()
  if descName == "" {
    return errors.New("parse current file error")
  }
  
  if err := set.parseDescFile(string(descName)); err != nil {
    return err
  }
  set.descNum = set.NewFileNumber()
  return nil
}

// return the current version within the version set
func (set *VersionSet) Current() *Version {
  return set.current
}
  
// Return the current manifest file number
func (set *VersionSet) ManifestFileNumber() int {
  return set.descNum
}
  
// Allocate and return a new file number
func (set *VersionSet) NewFileNumber() int {
  set.fileNum++
  return set.fileNum
}
  
// Arrange to reuse "file_number" unless a newer file number has
// already been allocated.
// REQUIRES: "file_number" was returned by a call to NewFileNumber().
func (set *VersionSet) ReuseFileNumber(num int) error {
  if num + 1 != set.fileNum {
    return errors.New("cound not reuse")
  }
  set.fileNum = num
  return nil
}
  
// Return the number of TAble files at the specified level
func (set *VersionSet) NumLevelFiles(level int) int {
  if level >= util.Global.MaxLevel || set.current == nil {
    return 0
  }
  return len(set.current.files[level])
}
  
// Return the combined file size of all files at the specified level
func (set *VersionSet) NumLevelBytes(level int) int {
  if level >= util.Global.MaxLevel || set.current == nil {
    return 0
  }
  return TotalFileSize(set.current.files[level])
}
  
// Set the last sequence number to seq
func (set *VersionSet) SetLastSequence(seq uint64) error {
  if seq < set.lastSeq {
    return errors.New("seq smaller than current")
  }
  set.lastSeq = seq
  return nil
}
  
// Return the last sequence number within the version set
func (set *VersionSet) LastSequence() uint64 {
  return set.lastSeq
}
  
// Mark the specified file number as used
func (set *VersionSet) MarkFileNumberUsed(num int) {
  if num > set.fileNum {
    set.fileNum = num
  }
}
  
// Return the current log file number
func (set *VersionSet) LogNumber() int {
  return set.logNum
}

func (set *VersionSet) SetLogNumber(num int) {
  set.logNum = num
}

// Pick level and inputs for a new compaction.
// Returns NULL if there is no compaction to be done.
// Otherwise returns a pointer to a heap-allocated object that
// describes the compaction.  Caller should delete the result.
func (set *VersionSet) PickCompaction() *compact.Compact {
   if set.current.cscore < 1 {
     return nil
   } 
   comp := compact.NewCompact()
   comp.Level = set.current.clevel
   
   if set.current.files[comp.Level] == nil || len(set.current.files[comp.Level]) == 0 {
     return nil
   }
   
   meta := set.current.files[comp.Level][0]
   if comp.Level == 0 {
     comp.Files[0] = set.current.GetOverlappingInputs(comp.Level, &meta.Smallest, &meta.Largest)
     small, large := set.getRange(comp.Files[0])
     comp.Files[1] = set.current.GetOverlappingInputs(comp.Level + 1, small, large)
   } else {
     comp.Files[0] = []*table.FileMetaData{meta}
     comp.Files[1] = set.current.GetOverlappingInputs(comp.Level + 1, &meta.Smallest, &meta.Largest)
   }
   
   var files []*table.FileMetaData
   files = append(files, comp.Files[0]...)
   files = append(files, comp.Files[1]...)
   comp.Smallest, comp.Largest = set.getRange(files)
   return comp
}

// Return the maximum overlapping data (in bytes) at next level for any
// file at a level >= 1.
func (set *VersionSet) MaxNextLevelOverlappingBytes() int {
  rslt := 0
  for i := 1; i < util.Global.MaxLevel - 1; i++ {
    for _, meta := range set.current.files[i] {
      inputs := set.current.GetOverlappingInputs(i + 1, &meta.Smallest, &meta.Largest)
      tmp := TotalFileSize(inputs)
      if tmp > rslt {
        rslt = tmp
      }
    } 
  }
  return rslt
}
  
// Create an iterator that reads over the compaction inputs for "*c".
// The caller should delete the iterator when no longer needed.
func (set *VersionSet) MakeInputIterator(c *compact.Compact) mem.Iterator {
  var iters []mem.Iterator
  for i := 0; i < 2; i++ {
    if c.Level + i == 0 {
      for j := 0; j < len(c.Files[i]); j++ {
        _, iter := set.cache.NewIterator(c.Files[i][j].Number, c.Files[i][j].FileSize)
        iters = append(iters, iter)
      }
      continue
    }
    idxIter := NewFilesIterator(c.Files[i])
    iter := table.NewTwoLevelIterator(idxIter, set.newFileIterator, nil, TableFileCompare)
    iters = append(iters, iter)
  }
  return table.NewMergeIterator(set.option.Comparator, iters) 
}
  
// Return a compaction object for compacting the range [begin,end] in
// the specified level.  Returns NULL if there is nothing in that
// level that overlaps the specified range.  Caller should delete
// the result.
func (set *VersionSet) CompactRange(level int, begin, end *util.InternalKey) {
  
}
  
// Returns true iff some level needs a compaction.
func (set *VersionSet) NeedsCompaction() bool {
  return set.current.cscore >= 1
}
 
// Get all files listed in any live version
// May also mutate some internal state.
func (set *VersionSet) GetLiveFiles() []int {
  ver := set.current
  rslt := []int{}
  for ver != nil {
    for i := 0; i < util.Global.MaxLevel; i++ {
      for _, meta := range ver.files[i] {
        rslt = append(rslt, meta.Number)
      }
    }
    ver = ver.prev
  }
  return rslt
}

// Return the approximate offset in the database of the data for
// "key" as of version "v".
func (set *VersionSet) ApproximateOffsetOf(v Version, key *util.InternalKey) int {
  return 0
}
  
// Return the name of db
func (set *VersionSet) DBName() string {
  return set.dbname
}
  
// Table cache of the db
func (set *VersionSet) TableCache() table.TableCache {
  return set.cache
}
  
// options set with the db  
func (set *VersionSet) Option() *util.Option {
  return set.option
}

func (set *VersionSet) scoreCompaction(v *Version) (float32, int) {
  var compLevel int = 0
  var compScore, tmpScore float32 = 0, 0
  for i := 0; i < util.Global.MaxLevel - 1; i++ {
    if i == 0 {
      clen := len(v.files[i])
      tmpScore = float32(clen) / float32(util.Global.L0CompactionTrigger)
    } else {
      tmpScore = float32(TotalFileSize(v.files[i])) / MaxBytesForLevel(i)
    }
    
    if tmpScore > compScore {
      compLevel = i
      compScore = tmpScore
    }
  }
  return compScore, compLevel
}

func (set *VersionSet) parseCurrentFile() string {
  curfile := util.CurrentFileName(set.dbname)
  if file, err := os.OpenFile(curfile, os.O_RDONLY, 0); err != nil {
    return ""
  } else {
    defer file.Close()
    reader := bufio.NewReader(file)
    if data, err := reader.ReadBytes('\n'); err != nil {
      return ""
    } else {
      return string(data[:len(data) - 1])
    }
  }
}

func (set *VersionSet) parseDescFile(descName string) error {
  reader, err := log.NewReader(descName, false, 0)
  if err != nil {
    return err
  }

  edit := NewVersionEdit()
  builder := NewVersionBuilder(set.current, set.option.Comparator)

  for true {
    if data, err := reader.Read(); err == io.EOF {
      builder.Finish(set.current)
      return nil
    } else if err != nil {
      return err 
    } else if err = edit.Decode(data); err != nil {
      return err
    } else {
      edit.Decode(data)
      if edit.LogNumber != -1 {
        set.logNum = edit.LogNumber
      }
      
      if edit.Sequence != 0 {
        set.lastSeq = edit.Sequence
      }
      
      if edit.FileNumber != -1 {
        set.fileNum = edit.FileNumber
      }
      
      if edit.CmpName != "" && edit.CmpName != set.option.Comparator.Name() {
        return errors.New("comparator name not match with older one")
      }
      builder.Apply(edit)
    }
  }
  return nil
}

func (set *VersionSet) getRange(files []*table.FileMetaData) (*util.InternalKey, *util.InternalKey) {
  var small, large *util.InternalKey = nil, nil
  icmp := set.option.Comparator
  for _, meta := range files {
    if small == nil {
      small, large = &meta.Smallest, &meta.Largest
      continue
    }
    
    if icmp.Compare(small.Encode(), meta.Smallest.Encode()) > 0 {
      small = &meta.Smallest
    } 
    
    if icmp.Compare(large.Encode(), meta.Largest.Encode()) < 0 {
      large = &meta.Largest
    }
  }
  return small, large
}

func (set *VersionSet) newFileIterator(meta interface{}) mem.Iterator {
  val := meta.(*table.FileMetaData)
  _, iter := set.cache.NewIterator(val.Number, val.FileSize)
  return iter
}

// Append another version into version set
func (set *VersionSet) append(v *Version) {
  set.current.next = v
  v.prev = set.current
  v.next = nil
  set.current = v 
}

func (set *VersionSet) writeSnapshot() error {
  edit := NewVersionEdit()
  edit.SetComparatorName(set.option.Comparator.Name())
  
  for i := 0; i < util.Global.MaxLevel; i++ {
    for j := 0; j < len(set.current.files[i]); j++ {
      meta := set.current.files[i][j]
      edit.AddFile(i, meta.Number, meta.FileSize, &meta.Smallest, &meta.Largest)
    }
  }
  return set.writer.AddRecord(edit.Encode())
}

func (set *VersionSet) setVersionEdit(edit *VersionEdit) {
  if edit.LogNumber == -1 {
    edit.SetLogNumber(set.logNum)
  }
  
  if edit.Sequence == 0 {
    edit.SetLastSequence(set.lastSeq)
  }
  
  if edit.CmpName == "" {
    edit.SetComparatorName(set.option.Comparator.Name())
  }
  
  if edit.FileNumber == -1 {
    edit.SetNextFile(set.fileNum)
  }
}

func (set *VersionSet) dumpCurrent() string {
  var buffer bytes.Buffer
  buffer.WriteString(fmt.Sprintf("Printing current version information\n"))
  buffer.WriteString(fmt.Sprintf("LogNumber:\t%d\n", set.LogNumber()))
  buffer.WriteString(fmt.Sprintf("FileNumber:\t%d\n", set.fileNum))
  buffer.WriteString(fmt.Sprintf("Sequence:\t%d\n", set.LastSequence()))
  buffer.WriteString(fmt.Sprintf("Desc:\t%d\n", set.descNum))
  buffer.WriteString(fmt.Sprintf("Printing Files\n"))
  for i := 0; i < util.Global.MaxLevel; i++ {
    buffer.WriteString(fmt.Sprintf("#####Level %d####\n", i))
    for _, meta := range set.current.files[i] {
      msg := fmt.Sprintf("| %d %s %s |", meta.Number, meta.Smallest.UserKey(),
        meta.Largest.UserKey())
      buffer.WriteString(msg)
    }
    buffer.WriteString("\n")
  }
  return buffer.String()
}
