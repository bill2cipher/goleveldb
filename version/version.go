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

func NewVersion(vset *VersionSet) *Version {
  v := new(Version)
  v.init(vset)
  return v  
}

type Version struct {
  files [][]*table.FileMetaData
  cscore float32    // compaction score
  clevel int        // compaction level 
  
  slevel int           // seek compaction level
  sfile  *table.FileMetaData // seek compaction file
  
  vset   *VersionSet    // version set this version associated with
  next   *Version       // next version within the set
  prev   *Version       // prev version within the set
}


func (v *Version) init(vset *VersionSet) {
  v.vset = vset
  v.cscore, v.clevel = 0, 0
  v.slevel, v.sfile = 0, nil

  v.next, v.prev = nil, nil

  v.files = make([][]*table.FileMetaData, util.Global.MaxLevel)
  for i := 0; i < util.Global.MaxLevel; i++ {
    v.files[i] = []*table.FileMetaData{}
  }
}

// Append to iters a sequence of iterators that will
// yield the contents of this Version when merged together.
// REQUIRES: This version has been saved (see VersionSet::SaveTo)
func (v *Version) GetIterators(option *util.ReadOption) []mem.Iterator {
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
    fiter := NewFilesIterator(v.files[i])
    iter := table.NewTwoLevelIterator(fiter, v.newTableIterator, option, TableFileCompare)
    rslt = append(rslt, iter)
  }
  return rslt
}

// Lookup the value for key.  If found, store it in *val and
// return OK.  Else return a non-OK status.  Fills *stats.
// REQUIRES: lock is not held 
func (v *Version) Get(option *util.ReadOption, key util.LookupKey) ([]byte, error) {
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
        
        if ucmp.Compare(ukey, file.Smallest.UserKey) < 0 {
          continue
        }
        search = append(search, file)
        sort.Sort(util.NewSliceSorter(search, TableFileCompare))
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


func (v *Version) GetOverlappingInputs(level int, begin, end *util.InternalKey) []*table.FileMetaData {
  if level >= util.Global.MaxLevel {
    return nil
  }
  rslt := []*table.FileMetaData{}
  ucmp := v.vset.Option().Comparator.(*mem.InternalKeyComparator).UserComparator()
  var ubegin, uend []byte
  if begin != nil {
    ubegin = begin.UserKey()
  } 
  if end != nil {
    uend = end.UserKey()
  }

  for i := 0; i < len(v.files[level]); i++ {
    file := v.files[level][i]
    if begin != nil && ucmp.Compare(ubegin, file.Largest.UserKey()) > 0 {
      continue
    }
    
    if end != nil && ucmp.Compare(uend, file.Smallest.UserKey()) < 0 {
      continue
    }
    
    rslt = append(rslt, file)
    if level != 0 {
      continue
    }
    
    if begin != nil && ucmp.Compare(ubegin, file.Smallest.UserKey()) > 0 {
      ubegin = file.Smallest.UserKey()
      i = 0
    } 
    
    if end != nil && ucmp.Compare(uend, file.Largest.UserKey()) < 0 {
      uend = file.Largest.UserKey()
      i = 0
    }
  }
  
  return rslt
}

// Returns true iff some file in the specified level overlaps
// some part of [*smallest_user_key,*largest_user_key].
// smallest_user_key==NULL represents a key smaller than all keys in the DB.
// largest_user_key==NULL represents a key largest than all keys in the DB.
func (v *Version) OverlapInLevel(level int, smallest, largest []byte) bool {
  if level >= util.Global.MaxLevel {
    return false
  }
  
  ucmp := v.vset.Option().Comparator.(*mem.InternalKeyComparator).UserComparator()
  for _, file := range v.files[level] {
    if largest != nil && ucmp.Compare(file.Smallest.UserKey(), largest) > 0 {
      continue
    }
    
    if smallest != nil && ucmp.Compare(file.Largest.UserKey(), smallest) < 0 {
      continue
    }
    return true
  }
  return false
}

// Return the level at which we should place a new memtable compaction
// result that covers the range [smallest_user_key,largest_user_key].  
func (v *Version) PickLevelForMemTableOutput(smallest, largest []byte) int {
  level := 0
  if v.OverlapInLevel(level, smallest, largest) {
    return level
  }
  
  for level + 1 < util.Global.MaxLevel {
    if v.OverlapInLevel(level + 1, smallest, largest) {
      break
    }
    
    if level >= util.Global.MaxMemCompactLevel {
      break
    }
    level++
  }
  return level
}

// File number at the specified level
func (v *Version) NumFiles(level int) int {
  return len(v.files[level])
}

// Call handler(arg, level, f) for every file that overlaps user_key in
// order from newest to oldest.  If an invocation of func returns
// false, makes no more calls.
//
// REQUIRES: user portion of internal_key == user_key.
func (v *Version) ForEachOverlapping(userKey, internalKey []byte, handle Handler) {
  
}

// Record a sample of bytes read at the specified internal key.
// Samples are taken approximately once every config::kReadBytesPeriod
// bytes.  Returns true if a new compaction may need to be triggered.
// REQUIRES: lock is held
func (v *Version) RecordReadSample(key []byte) bool {
  return false
}


func (v *Version) newTableIterator(meta interface{}) mem.Iterator {
  table := meta.(*table.FileMetaData)
  _, iter := v.vset.TableCache().NewIterator(table.Number, table.FileSize)
  return iter
}
