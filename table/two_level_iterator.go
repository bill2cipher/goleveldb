package table

import (
  "github.com/goleveldb/mem"
  "github.com/goleveldb/util"
)

type twoLevelIterator struct {
  index  mem.Iterator
  next   NewIterator
  option util.ReadOption
  data   mem.Iterator
  current interface{}
  cmp    util.Comparator
}

type NewIterator func(idx interface{}) mem.Iterator

func NewTwoLevelIterator(index mem.Iterator, next NewIterator, option util.ReadOption, cmp util.Comparator) mem.Iterator {
  iter := new(twoLevelIterator)
  iter.init(index, next, option, cmp)
  return iter
}

func (t *twoLevelIterator) init(index mem.Iterator, next NewIterator, option util.ReadOption, cmp util.Comparator) {
  t.index = index
  t.data  = nil
  t.next  = next
  t.option = option
  t.cmp = cmp
}

func (t *twoLevelIterator) Valid() bool {
  if t.data == nil {
    return false
  }
  return t.data.Valid()
}

func (t *twoLevelIterator) Key() interface{} {
  if !t.Valid() {
    return nil
  }
  return t.data.Key()
}

func (t *twoLevelIterator) Value() interface{} {
  if !t.Valid() {
    return nil
  }
  return t.data.Value()
}

func (t *twoLevelIterator) Next() {
  if !t.Valid() {
    return
  }
  t.data.Next()
  t.SkipEmptyDataIterForward()
}

func (t *twoLevelIterator) Prev() {
  if !t.Valid() {
    return
  }
  t.data.Prev()
  t.SkipEmptyDataIterBackward()
}

func (t *twoLevelIterator) Seek(key interface{}) {
  t.index.Seek(key)
  t.InitDataIterator()
  if t.data != nil {
    t.data.Seek(key)
  }
  t.SkipEmptyDataIterForward()
}

func (t *twoLevelIterator) SeekToFirst() {
  t.index.SeekToFirst()
  t.InitDataIterator()
  if t.data != nil {
    t.data.SeekToFirst()
  }
  t.SkipEmptyDataIterForward()
}

func (t *twoLevelIterator) SeekToLast() {
  t.index.SeekToLast()
  t.InitDataIterator()
  if t.data != nil {
    t.data.SeekToLast()
  }
  t.SkipEmptyDataIterBackward()
}

func (t *twoLevelIterator) InitDataIterator() {
  if !t.index.Valid() {
    t.data = nil
    return
  }
  
  handler := t.index.Value()
  if t.data != nil && t.cmp.Compare(handler, t.current) == 0 {
    return
  }
  
  t.data = t.next(handler)
  t.current = handler 
}

func (t *twoLevelIterator) SkipEmptyDataIterForward() {
  for t.data == nil || !t.data.Valid() {
    if !t.index.Valid() {
      t.data = nil
      return
    }
    
    t.index.Next()
    t.InitDataIterator()
    if t.data != nil {
      t.data.SeekToFirst()
    }
  }
}

func (t *twoLevelIterator) SkipEmptyDataIterBackward() {
  for t.data == nil || !t.data.Valid() {
     if !t.index.Valid() {
       t.data = nil
       return
     }
     
     t.index.Prev()
     t.InitDataIterator()
     if t.data != nil {
       t.data.SeekToLast()
     }
  }
}
