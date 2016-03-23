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
    if rslt := cmp.Compare(key, meta.Largest); rslt > 0 {
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

func CompareTableFile(f, s interface{}) bool {
  fmeta := f.(*table.FileMetaData)
  smeta := s.(*table.FileMetaData)
  return fmeta.Number < smeta.Number
}

func NewFilesIterator(data interface{}) mem.Iterator {
  iter := new(sliceIterator)
  iter.value = data.([])
  iter.cur = -1
  return iter
}

type sliceIterator struct {
  value []interface{}
  cur   int
}

func (s *sliceIterator) Valid() bool {
  return s.cur >= 0 && s.cur < len(s.value)
}

func (s *sliceIterator) Key() interface{} {
  if !s.Valid() {
    return nil
  }
  return s.cur
}

func (s *sliceIterator) Value() interface{} {
  if !s.Valid() {
    return nil
  }
  return s.value[s.cur]
}

func (s *sliceIterator) Next() {
  s.cur++
}

func (s *sliceIterator) Prev() {
  s.cur--
}

func (s *sliceIterator) Seek(key interface{}) {
  vkey := key.(int)
  s.cur = vkey
}

func (s *sliceIterator) SeekToFirst() {
  s.cur = 0
}

func (s *sliceIterator) SeekToLast() {
  slen := len(s.value)
  s.cur = slen - 1
}
