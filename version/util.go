package version

import (
  "github.com/jellybean4/goleveldb/mem"
  "github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/table"
)

func FindTable(cmp util.Comparator, files []*table.FileMetaData, key []byte) *table.FileMetaData {
  left, right := 0, len(files)
  for left < right {
    mid := (left + right) / 2
    meta := files[mid]
    if rslt := cmp.Compare(key, meta.Largest.Encode()); rslt > 0 {
      left = mid + 1
    } else {
      right = mid
    } 
  }
  
  if left == len(files) {
    return nil
  }
  return files[left]
}

func TableFileCompare(f, s interface{}) int {
  fmeta := f.(*table.FileMetaData)
  smeta := s.(*table.FileMetaData)
  if fmeta.Number < smeta.Number {
    return -1
  } else if fmeta.Number == smeta.Number {
    return 0
  } else {
    return 1
  }
}

func NewFilesIterator(data []*table.FileMetaData) mem.Iterator {
  iter := new(filesIterator)
  iter.value = data
  iter.cur = -1
  return iter
}

func TotalFileSize(files []*table.FileMetaData) int {
  rslt := 0
  for _, f := range files {
    rslt += f.FileSize
  }
  return rslt
}

func MaxBytesForLevel(level int) float32 {
  var rslt float32 = 10 * 1048576
  for level > 1 {
    rslt *= 10
    level--
  }
  return rslt
}

type filesIterator struct {
  value []*table.FileMetaData
  cur   int
}

func (s *filesIterator) Valid() bool {
  return s.cur >= 0 && s.cur < len(s.value)
}

func (s *filesIterator) Key() interface{} {
  if !s.Valid() {
    return nil
  }
  return s.cur
}

func (s *filesIterator) Value() interface{} {
  if !s.Valid() {
    return nil
  }
  return s.value[s.cur]
}

func (s *filesIterator) Next() {
  s.cur++
}

func (s *filesIterator) Prev() {
  s.cur--
}

func (s *filesIterator) Seek(key interface{}) {
  vkey := key.(int)
  s.cur = vkey
}

func (s *filesIterator) SeekToFirst() {
  s.cur = 0
}

func (s *filesIterator) SeekToLast() {
  slen := len(s.value)
  s.cur = slen - 1
}
