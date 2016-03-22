package version

import (
	"github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/mem"
  "github.com/jellybean4/goleveldb/table"
)




type Handler func(args []interface{}, level int, meta *FileMetaData) bool

type Version interface {
  // Append to iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  GetIterators(option *util.ReadOption) []mem.Iterator

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held 
  Get(option *util.ReadOption, key util.LookupKey) ([]byte, error)
  
  GetOverlappingInputs(level int, begin, end *util.InternalKey) []*FileMetaData
  
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

type versionImpl struct {
  files [][]*FileMetaData
  cscore float64    // compaction score
  clevel int        // compaction level 
  
  slevel int           // seek compaction level
  sfile  *FileMetaData // seek compaction file
  
  vset   VersionSet    // version set this version associated with
  next   Version       // next version within the set
  prev   Version       // prev version within the set
}

func (v *versionImpl) init() {
  v.files = make([][]*FileMetaData, util.Global.Max_Level)
  for i := 0; i < util.Global.Max_Level; i++ {
    v.files[i] = []*FileMetaData{}
  }
}

func (v *versionImpl) GetIterators(option *util.ReadOption) []mem.Iterator {
  rslt := []mem.Iterator{}
  level0 := v.files[0]
  for i := 0; i < len(level0); i++ {
    filename := util.TableFileName(v.vset.DBName(), uint64(level0[i].Number))
    table := table.OpenTable(filename, level0[i].FileSize, v.vset.Option())
  }
}

func (v *versionImpl) Get(option *util.ReadOption, key util.LookupKey) ([]byte, error) {
  return nil, nil  
}

func (v *versionImpl) GetOverlappingInputs(level int, begin, end *util.InternalKey) []*FileMetaData {
  return nil
}

func (v *versionImpl) OverlapInLevel(level int, smallest, largest []byte) bool {
  return false
}

func (v *versionImpl) PickLevelForMemTableOutput(smallest, largest []byte) int {
  return 0
}

func (v *versionImpl) NumFiles(level int) int {
  return 0
}

func (v *versionImpl) ForEachOverlapping(userKey, internalKey []byte, handle Handler) {
  
}

func (v *versionImpl) NewConcatingIterator(option *util.ReadOption, level int) mem.Iterator {
  return nil
}

func (v *versionImpl) RecordReadSample(key []byte) bool {
  return false
}

