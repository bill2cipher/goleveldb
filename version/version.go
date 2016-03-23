package version

import (
  "sort"
  "errors"
)

import (
	"github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/mem"
  "github.com/jellybean4/goleveldb/table"
)




type Handler func(args []interface{}, level int, meta *table.FileMetaData) bool

type Version interface {
  // Append to iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  GetIterators(option *util.ReadOption) []mem.Iterator

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held 
  Get(option *util.ReadOption, key util.LookupKey) ([]byte, error)
  
  GetOverlappingInputs(level int, begin, end *util.InternalKey) []* table.FileMetaData
  
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
  files [][]*table.FileMetaData
  cscore float64    // compaction score
  clevel int        // compaction level 
  
  slevel int           // seek compaction level
  sfile  *table.FileMetaData // seek compaction file
  
  vset   VersionSet    // version set this version associated with
  next   Version       // next version within the set
  prev   Version       // prev version within the set
}

func (v *versionImpl) init() {
  v.files = make([][]*table.FileMetaData, util.Global.MaxLevel)
  for i := 0; i < util.Global.MaxLevel; i++ {
    v.files[i] = []*table.FileMetaData{}
  }
}

func (v *versionImpl) GetIterators(option *util.ReadOption) []mem.Iterator {
  rslt := []mem.Iterator{}
  level0 := v.files[0]
  for i := 0; i < len(level0); i++ {
    _, iter := v.vset.TableCache().NewIterator(level0[i].Number, level0[i].FileSize);
    if iter == nil {
      return nil
    }
    rslt = append(rslt, iter)
  }
  
  for i := 1; i < util.Global.MaxLevel; i++ {
    fiter := NewSliceIterator(v.files[i])
    iter := table.NewTwoLevelIterator(fiter, v.newTableIterator, option, util.BinaryComparator)
    rslt = append(rslt, iter)
  }
  return rslt
}

func (v *versionImpl) newTableIterator(meta interface{}) mem.Iterator {
  table := meta.(*table.FileMetaData)
  _, iter := v.vset.TableCache().NewIterator(table.Number, table.FileSize)
  return iter
}

func (v *versionImpl) Get(option *util.ReadOption, key util.LookupKey) ([]byte, error) {
  cmp  := v.vset.Option().Comparator
  ucmp := cmp.(*mem.InternalKeyComparator).UserComparator()
  ikey := key.InternalKey()
  ukey := key.UserKey()
  for i := 0; i < util.Global.MaxLevel; i++ {
    var search []interface{}
    if i == 0 {
      for _, file := range v.files[i] {
        if ucmp.Compare(ukey, file.Largest.UserKey()) > 0 {
          continue
        }
        
        if cmp.Compare(ukey, file.Smallest.UserKey) < 0 {
          continue
        }
        search = append(search, file)
        sort.Sort(util.NewSliceSorter(search, CompareTableFile))
      }
    } else {
      file := FindTable(cmp, v.files[i], key.InternalKey())
      if file == nil {
        continue
      }
      search = append(search, file)
    }
    
    for k := 0; k < len(search); k++ {
      meta := search[k].(*table.FileMetaData)
      skey, sval := v.vset.TableCache().Get(option, meta.Number, meta.FileSize, ikey)
      if skey == nil || sval == nil {
        continue
      }
      
      if ucmp.Compare(util.ExtractUserKey(skey), ukey) == 0 {
        return sval, nil
      }
    }
  }
  return nil, errors.New("could not find target")  
}

func (v *versionImpl) GetOverlappingInputs(level int, begin, end *util.InternalKey) []*table.FileMetaData {
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
