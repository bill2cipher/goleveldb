package version

import (
  "bytes"
  "errors"
  "encoding/binary"
)

import (
	"github.com/goleveldb/util"
  "github.com/goleveldb/mem"
)


type FileMetaData struct {
  AllowSeek int
  Number    int
  FileSize  int
  Smallest  util.InternalKey
  Largest   util.InternalKey
}

type Handler func(args []interface{}, level int, meta *FileMetaData) bool

type Version interface {
  // Append to iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  AddIterators(option *util.ReadOption, iters []mem.Iterator)

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held 
  Get(option *util.ReadOption, key util.LookupKey) ([]byte, error)
  
  GetOverlappingInputs(level int, begin, end *util.InternalKey, inputs []*FileMetaData)
  
  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==NULL represents a key smaller than all keys in the DB.
  // largest_user_key==NULL represents a key largest than all keys in the DB.
  OverlapInLevel(level int, smallest, largest []byte) bool

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].  
  PickLevelForMemTableOutput(smallest, largest []byte) int
  
  // File number at the specified level
  NumFiles(level int) int
  
  // Call handler(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  ForEachOverlapping(userKey, internalKey []byte, handle Handler)
  
  
  NewConcatenatingIterator(option *util.ReadOption, level int) mem.Iterator;
  
  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  RecordReadSample(key []byte) bool
}


func NewVersion(vset VersionSet) Version {
  return nil  
}
