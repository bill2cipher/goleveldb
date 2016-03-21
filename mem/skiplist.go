package mem

import (
  "math/rand"
  "sync/atomic"
)

import (
  "github.com/goleveldb/util"
)

func NewSkiplist(cmp util.Comparator) Skiplist {
  list := new(skiplistImpl)
  list.init(cmp)
  return list
}

type nodeImpl struct {
  value interface{}
  next  []atomic.Value
}

func (n *nodeImpl) nextValue(height int) *nodeImpl {
  if value := n.next[height].Load(); value == nil {
    return nil
  } else {
    return value.(*nodeImpl)
  }
}

func (n *nodeImpl) setNext(height int, value *nodeImpl) bool {
  if height < 0 || height >= len(n.next) {
    return false
  }
  n.next[height].Store(value)
  return true
}

const (
  MaxHeight = 12
)

type skiplistImpl struct {
  header *nodeImpl
  cmp util.Comparator
}

func (s *skiplistImpl) init(cmp util.Comparator) {
  s.cmp = cmp
  s.header = new(nodeImpl)
  s.header.value = nil
  s.header.next = make([]atomic.Value, MaxHeight)
}

func (s *skiplistImpl) Insert(value interface{}) bool {
  prev := make([]*nodeImpl, MaxHeight)
  if exists := s.findGreatOrEqual(value, prev); exists == nil {

  } else if s.cmp.Compare(exists.value, value) == 0 {
    return false
  }

  node := new(nodeImpl)
  node.value = value
  node.next = make([]atomic.Value, s.randomHeight())

  for i := 0; i < len(node.next); i++ {
    node.setNext(i, prev[i].nextValue(i))
    prev[i].setNext(i, node)
  }
  return true
}

func (s *skiplistImpl) Contains(value interface{}) bool {
  if node := s.findGreatOrEqual(value, nil); node == nil {
    return false
  } else if cmp := s.cmp.Compare(value, node.value); cmp != 0 {
    return false
  } else {
    return true
  }
}

func (s *skiplistImpl) DumpData() []interface{} {
  rslt := make([]interface{}, 0)
  current := s.header.nextValue(0)
  for current != nil {
    rslt = append(rslt, current.value)
    current = current.nextValue(0)
  }
  return rslt
}

func (s *skiplistImpl) randomHeight() int {
  // Increase height with probability 1 in Branching
  height := 1
  for (height < MaxHeight) && (rand.Int() % 4 == 0) {
    height++
  }
  return height
}

func (s *skiplistImpl) findLessThan(value interface{}, prev []*nodeImpl) *nodeImpl {
  current := s.header
  height := MaxHeight - 1
  for height >= 0 {
    if prev != nil {
      prev[height] = current
    }

    if next := current.nextValue(height); next == nil {
      height--
    } else if cmp := s.cmp.Compare(next.value, value); cmp >= 0 {
      height--
    } else if cmp < 0 {
      current = next
    }
  }
  return current 
}

func (s *skiplistImpl) findGreatOrEqual(value interface{}, prev []*nodeImpl) *nodeImpl {
  current := s.header
  height := MaxHeight - 1
  var next *nodeImpl = nil

  for height >= 0 {
    if prev != nil {
      prev[height] = current
    }

    if next = current.nextValue(height); next == nil {
      height--
    } else if cmp := s.cmp.Compare(next.value, value); cmp < 0 {
      current = next
    } else if cmp >= 0 {
      height--
    }
  }
  return next
}

func (s *skiplistImpl) findLast() *nodeImpl {
  height := MaxHeight - 1
  current := s.header
  for height >= 0 {
    if next := current.nextValue(height); next == nil {
      height--
    } else {
      current = next
    }
  }
  return current
}

func (s *skiplistImpl) NewIterator() Iterator {
  iter := new(iteratorImpl)
  iter.init(s)
  return iter
}

type iteratorImpl struct {
  current *nodeImpl
  list    *skiplistImpl
}

func (i *iteratorImpl) init(list *skiplistImpl) {
  i.list = list
}

func (i *iteratorImpl) Valid() bool {
  return i.current != nil
}

func (i *iteratorImpl) Key() interface{} {
  return i.current.value
}

func (i *iteratorImpl) Value() interface{} {
  return nil
}

func (i *iteratorImpl) Next() {
  i.current = i.current.nextValue(0)
}

func (i *iteratorImpl) Prev() {
  i.current = i.list.findLessThan(i.current.value, nil)
  if i.current == i.list.header {
    i.current = nil
  }
}

func (i *iteratorImpl) Seek(key interface{}) {
  i.current = i.list.findGreatOrEqual(key, nil)
}

func (i *iteratorImpl) SeekToFirst() {
  i.current = i.list.header.nextValue(0)
}

func (i *iteratorImpl) SeekToLast() {
  i.current = i.list.findLast()
  if i.current == i.list.header {
    i.current = nil
  }
}