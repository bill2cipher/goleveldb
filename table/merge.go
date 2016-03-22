package table

import (
	"github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/mem"
)

type mergeIterator struct {
  cmp      util.Comparator
  children []mem.Iterator
  current  int
  direct   int              // 0 means forward and 1 means backward
}

// Returns an iterator that provided the union of the data in
// children[0, n-1]. Takes ownership of the child iterators and
// 
// The result does no duplicate suppression. I.e, if a particular
// is present in K child iterators, it will be yileded K times.
func NewMergeIterator(cmp util.Comparator, children []mem.Iterator) mem.Iterator {
  if len(children) == 0 {
    return nil
  }
  
  if len(children) == 1 {
    return children[0]
  }
  
  iter := new(mergeIterator)
  iter.init(cmp, children)
  return iter
}

func (m *mergeIterator) init(cmp util.Comparator, children []mem.Iterator) {
  m.cmp = cmp
  m.children = children
  m.current = 0
  m.direct  = 0
}

func (m *mergeIterator) Valid() bool {
  for i := 0; i < len(m.children); i++ {
    if m.children[i].Valid() {
      return true
    }
  }
  return false
}

func (m *mergeIterator) Key() interface{} {
  iter := m.children[m.current]
  return iter.Key()
}

func (m *mergeIterator) Value() interface{} {
  iter := m.children[m.current]
  return iter.Value()
}

func (m *mergeIterator) Next() {
  if m.direct != 0 {
    current := m.Key()
    for i := 0; i < len(m.children); i++ {
      if i == m.current {
        continue
      }
      
      m.children[i].Seek(current)
      if !m.children[i].Valid() {
        continue
      }
      
      if key := m.children[i].Key(); m.cmp.Compare(key, current) == 0 {
        m.children[i].Next()
      }
    }
  }
  iter := m.children[m.current]
  iter.Next()
  m.current = m.findSmallest()
}

func (m *mergeIterator) Prev() {
  if m.direct != 1 {
    current := m.Key()
    
    for i := 0; i < len(m.children); i++ {
      if i == m.current {
        continue
      }
      m.children[i].Seek(current)
      if !m.children[i].Valid() {
        continue
      }
      m.children[i].Prev()
    }
  }
  
  iter := m.children[m.current]
  iter.Prev()
  m.current = m.findLargest()
}

func (m *mergeIterator) Seek(key interface{}) {
  for i := 0; i < len(m.children); i++ {
    m.children[i].Seek(key)
  }
  
  m.current = m.findSmallest()
  m.direct  = 0
}

func (m *mergeIterator) SeekToFirst() {
  for i := 0; i < len(m.children); i++ {
    m.children[i].SeekToFirst()
  }
  
  m.current = m.findSmallest()
  m.direct  = 0
}

func (m *mergeIterator) SeekToLast() {
  for i := 0; i < len(m.children); i++ {
    m.children[i].SeekToLast()
  }
  m.current = m.findLargest()
  m.direct  = 0
}

func (m *mergeIterator) findSmallest() int {
  var lastKey interface{} = nil
  idx := -1
   
  for i := 0; i < len(m.children); i++ {
    if !m.children[i].Valid() {
      continue
    }
    
    if lastKey == nil {
      lastKey = m.children[i].Key()
      idx = i
    }
    
    if cmp := m.cmp.Compare(lastKey, m.children[i].Key()); cmp > 0 {
      lastKey = m.children[i].Key()
      idx = i
    }
  }
  
  return idx
}

func (m *mergeIterator) findLargest() int {
  var lastKey interface{} = nil
  idx := -1
   
  for i := 0; i < len(m.children); i++ {
    if !m.children[i].Valid() {
      continue
    }
    
    if lastKey == nil {
      lastKey = m.children[i].Key()
      idx = i
    }
    
    if cmp := m.cmp.Compare(lastKey, m.children[i].Key()); cmp < 0 {
      lastKey = m.children[i].Key()
      idx = i
    }
  }
  
  return idx
}
