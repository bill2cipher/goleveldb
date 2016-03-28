package mem

import (
  "math/rand"
  "testing"
  "fmt"
  "sort"
  "github.com/jellybean4/goleveldb/util"
)

func TestRand(t *testing.T) {
  m := make(map[int]bool)
  mem := NewMemtable(NewInternalKeyComparator(util.BinaryComparator))
  for i := uint64(1); i < 50; i++ {
    data := rand.Int()
    key := []byte(fmt.Sprintf("key%d", data))
    val := []byte(fmt.Sprintf("val%d", data))
    if _, ok := m[data]; ok {
      continue
    }
    m[data] = true
    mem.Add(i, ValueType, key, val)
  }

  l := []int{}
  for i := range m {
    l = append(l, i)
  }
  sort.Ints(l)

  for _, i := range l {
    key := []byte(fmt.Sprintf("key%d", i))
    val := []byte(fmt.Sprintf("val%d", i))
    sKey := *util.NewLookupKey(key, util.Global.MaxSeq, SeekType)
    if rslt := mem.Get(sKey); rslt == nil {
      t.Errorf("could not find rand key %v", string(key))
    } else if util.BinaryCompare(val, rslt) != 0 {
      t.Errorf("find rand key not match %v %v", val, rslt)
    }
  }

  s := []string{}
  for _, i := range l {
    s = append(s, fmt.Sprintf("%d", i))
  }
  sort.Strings(s)

  iter := mem.NewIterator()
  count := 0
  iter.SeekToFirst()

  for iter.Valid() {
    key := []byte(fmt.Sprintf("key%s", s[count]))
    val := []byte(fmt.Sprintf("val%s", s[count]))

    if util.BinaryCompare(val, iter.Value().([]byte)) != 0 {
      t.Errorf("loop value not match %s %s", string(val), string(iter.Value().([]byte)))
    }

    iterKey := iter.Key().([]byte)
    if util.BinaryCompare(key, iterKey[ : len(iterKey) - 8]) != 0 {
      t.Errorf("loop key not match %s %s", string(key), string(iterKey[ : len(iterKey) - 8]))
    }
    count++
    iter.Next()
  }

  if count != len(l) {
    t.Errorf("iterator count not match %d %d", count, l)
  }
}

func TestMem(t *testing.T) {
  written := 0
  mem := NewMemtable(NewInternalKeyComparator(util.BinaryComparator))
  for i := uint64(1); i < 1000; i++ {
    key := []byte(fmt.Sprintf("key%d", i))
    val := []byte(fmt.Sprintf("val%d", i))
    mem.Add(i, ValueType, key, val)
    written += len(key) + len(val) + 16
  }

  if written != mem.ApproximateMemoryUsage() {
    t.Errorf("size calc error %d %d", written, mem.ApproximateMemoryUsage())
  }

  for i := uint64(1); i < 1000; i++ {
    key := []byte(fmt.Sprintf("key%d", i))
    val := []byte(fmt.Sprintf("val%d", i))
    searchKey := *util.NewLookupKey(key, util.Global.MaxSeq, SeekType)

    if rslt := mem.Get(searchKey); rslt == nil {
      t.Errorf("could not find exist key %s", key)
    } else if util.BinaryCompare(val, rslt) != 0 {
      t.Errorf("find exist key not match %v %v", val, rslt)
    }
  }

  for i := uint64(1); i < 4; i++ {
    key := []byte(fmt.Sprintf("not%d", i))
    sKey := *util.NewLookupKey(key, 0, SeekType)

    if rslt := mem.Get(sKey); rslt != nil {
      t.Errorf("find not match key %v %v", rslt, key)
    }
  }
}
