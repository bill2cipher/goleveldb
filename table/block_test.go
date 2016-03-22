package table

import (
  "testing"
  "fmt"
  "sort"
)

import (
  "github.com/jellybean4/goleveldb/util"
)

func TestBlock(t *testing.T) {
  builder := NewBlockBuilder(16)
  keys := make([]string, 1000)
  vals := make([]string, 1000)
  for i := 0; i < 1000; i++ {
    k := fmt.Sprintf("key%d", i)
    v := fmt.Sprintf("val%d", i)
    keys[i] = k
    vals[i] = v
  }
  sort.Strings(keys)
  sort.Strings(vals)

  if !builder.Empty() {
    t.Errorf("new builder not empty")
  }

  for i := 0; i < 1000; i++ {
    builder.Add([]byte(keys[i]), []byte(vals[i]))
  }

  if builder.Empty() {
    t.Errorf("full builder empty")
  }
  size := builder.CurrentSizeEstimate()
  rslt := builder.Finish()

  if size != len(rslt) {
    t.Errorf("precalc size not the same with real size %d %d", size, len(rslt))
  }

  builder.Reset()
  for i := 0; i < 1000; i++ {
    builder.Add([]byte(keys[i]), []byte(vals[i]))
  }
  rslt2 := builder.Finish()

  if len(rslt) != len(rslt2) {
    t.Errorf("reset rslt len not match %d %d", len(rslt), len(rslt2))
  }  

  for i := 0; i < len(rslt); i++ {
    if rslt[i] != rslt2[i] {
      t.Errorf("reset rslt not match")
      break
    }
  }

  reader := NewBlock(rslt)
  if reader.Size() != size {
    t.Errorf("reader size not the same with precalc size %d %d", reader.Size(), size)
  }

  iter := reader.NewIterator(util.BinaryComparator)
  if iter.Valid() {
    t.Errorf("new iter valid")
  }

  iter.SeekToFirst()
  if !iter.Valid() {
    t.Errorf("iter not valid after seek to first")
  }

  for i := 0; i < 1000; i++ {
    if !iter.Valid() {
      t.Errorf("iter not valid before parse")
      break
    }

    if string(iter.Key().([]byte)) != keys[i] {
      t.Errorf("iter key not match %s %s", string(iter.Key().([]byte)), keys[i])
    }

    if string(iter.Value().([]byte)) != vals[i] {
      t.Errorf("iter val not match %s %s", string(iter.Value().([]byte)), vals[i])
    }
    iter.Next()
  }

  if iter.Valid() {
    t.Errorf("iter valid after 1000 next")
  }

  iter.SeekToLast()
  if !iter.Valid() {
    t.Errorf("iter not valid after seek to last")
  }

  for i := 999; i >= 0; i-- {
    if !iter.Valid() {
      t.Errorf("iter not valid before parse2 %d", i)
    }

    if string(iter.Key().([]byte)) != keys[i] {
      t.Errorf("iter2 key not match %d %s %s", i, string(iter.Key().([]byte)), keys[i])
    }

    if string(iter.Value().([]byte)) != vals[i] {
      t.Errorf("iter2 val not match %d %s %s", i, string(iter.Value().([]byte)), vals[i])
    }
    iter.Prev()
  }
  
  for i := 0; i < 1000; i++ {
    key := fmt.Sprintf("key%d", i)
    val := fmt.Sprintf("val%d", i)
    iter.Seek([]byte(key))
    if !iter.Valid() {
      t.Errorf("could not find %s", key)
    } else if string(iter.Key().([]byte)) != key {
      t.Errorf("seek %s key not match %s", key, string(iter.Key().([]byte)))
    } else if string(iter.Value().([]byte)) != val {
      t.Errorf("seek %s value not match %s", key, iter.Value().([]byte))
    }
  }

  for i := 0; i < 10; i++ {
    key := fmt.Sprintf("ley%d", i)
    iter.Seek([]byte(key))
    if iter.Valid() {
      t.Errorf("could find %s", key)
    }
  }

  for i := 0; i < 10; i++ {
    key := fmt.Sprintf("aey%d", i)
    iter.Seek([]byte(key))
    if !iter.Valid() {
      t.Errorf("could find %s", key)
    }
  }
}
