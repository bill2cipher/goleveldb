package table

import (
  "os"
  "fmt"
  "sort"
  "testing"
)

import (
  "github.com/goleveldb/util"
)

func TestTableBuild(t *testing.T) {
  cnt := 100000
  filename := "/tmp/test.dat"
  keys := make([]string, cnt)
  vals := make([]string, cnt)
  
  for i := 0; i < cnt; i++ {
    key := fmt.Sprintf("key%d", i)
    val := fmt.Sprintf("val%d", i)
    keys[i] = key
    vals[i] = val
  }
  sort.Strings(keys)
  sort.Strings(vals)
  
  
  builder := NewTableBuilder(filename, util.DefaultOption)
  for i := 0; i < cnt; i++ {
    builder.Add([]byte(keys[i]), []byte(vals[i]))
    if builder.Status() != OK {
      t.Errorf("builder status %d after add", builder.Status())
    }
  }
  builder.Finish()
  defer os.Remove(filename)
  if builder.NumEntries() != cnt {
    t.Errorf("builder entries cnt wrong %d", builder.NumEntries())
  }
  
  table := OpenTable(filename, builder.FileSize(), util.DefaultOption)
  if table == nil {
    t.Errorf("open table %s failed", filename)
    return
  }
  
  iter := table.NewIterator()
  iter.SeekToFirst()
  if !iter.Valid() {
    t.Errorf("iter seek to first not valid")
  }
  j := 0
  for iter.Valid() {
    key := iter.Key().([]byte)
    val := iter.Value().([]byte)
    if string(key) != keys[j] {
      t.Errorf("iter next key not match %s %s", key, keys[j])
    }
    
    if string(val) != vals[j] {
      t.Errorf("iter next val not match %s %s", val, vals[j])
    }
    j++
    iter.Next()
  }
  
  if j < cnt {
    t.Errorf("iter next cnt %d less than 100000", j)
  }
  
  iter.SeekToLast()
  if !iter.Valid() {
    t.Errorf("iter seek to last not valid")
    return
  }
  j = cnt - 1
  last := []byte{}
  for j >= 0 {
    key := iter.Key().([]byte)
    val := iter.Value().([]byte)
    if string(key) != keys[j] {
      t.Errorf("iter prev key not match %s %s", key, keys[j])
    }
    
    if string(val) != vals[j] {
      t.Errorf("iter prev val not match %s %s", val, vals[j])
    }
    if string(last) == string(val) {
      t.Errorf("iter last is the same %s", last)
    }
    last = val
    j--
    iter.Prev()
    
    if !iter.Valid() && j >= 0 {
      t.Errorf("iter prev not valid")
      break
    }
  }
  
  for i := 0; i < cnt; i++ {
    key := fmt.Sprintf("key%d", i)
    val := fmt.Sprintf("val%d", i)
    iter.Seek([]byte(key))
    if !iter.Valid() {
      t.Errorf("iter seek val %s invalid", key)
      break
    }
    
    if goal := iter.Key().([]byte); string(goal) != key {
      t.Errorf("iter seek val %s / %s not found", key, goal)
    } 
    
    if goal := iter.Value().([]byte); string(goal) != val {
      t.Errorf("iter seek val %s / %s not found", val, goal)
    }
  }  
  defer table.Close()

  if builder.Status() != FINISH {
    t.Errorf("builder status %d after finish", builder.Status())
  }

  filename = "/tmp/test.dat2"
  builder = NewTableBuilder(filename, util.DefaultOption)
  for i := 0; i < cnt; i++ {
    key := keys[i]
    val := vals[i]
    builder.Add([]byte(key), []byte(val))
    if i % 100 == 99 {
      builder.Flush()
    }
    if builder.Status() != OK {
      t.Errorf("builder status %d after add or flush", builder.Status())
    }
  }

  if builder.NumEntries() != cnt {
    t.Errorf("buidler entries cnt wrong %d", builder.NumEntries())
  }
  builder.Finish()
  
  table = OpenTable(filename, builder.FileSize(), util.DefaultOption)
  if table == nil {
    t.Errorf("open table %s failed", filename)
    return
  }
  iter = table.NewIterator()
  iter.SeekToFirst()
  if !iter.Valid() {
    t.Errorf("iter seek to first not valid")
  }
  j = 0
  for iter.Valid() {
    key := iter.Key().([]byte)
    val := iter.Value().([]byte)
    if string(key) != keys[j] {
      t.Errorf("iter next key not match %s %s", key, keys[j])
    }
    
    if string(val) != vals[j] {
      t.Errorf("iter next val not match %s %s", val, vals[j])
    }
    j++
    iter.Next()
  }
  
  if j < cnt {
    t.Errorf("iter next cnt %d less than 100000", j)
  }
  
  iter.SeekToLast()
  if !iter.Valid() {
    t.Errorf("iter seek to last not valid")
    return
  }
  j = cnt - 1
  last = []byte{}
  for j >= 0 {
    key := iter.Key().([]byte)
    val := iter.Value().([]byte)
    if string(key) != keys[j] {
      t.Errorf("iter prev key not match %s %s", key, keys[j])
    }
    
    if string(val) != vals[j] {
      t.Errorf("iter prev val not match %s %s", val, vals[j])
    }
    if string(last) == string(val) {
      t.Errorf("iter last is the same %s", last)
    }
    last = val
    j--
    iter.Prev()
    
    if !iter.Valid() && j >= 0 {
      t.Errorf("iter prev not valid")
      break
    }
  }
  
  for i := 0; i < cnt; i++ {
    key := fmt.Sprintf("key%d", i)
    val := fmt.Sprintf("val%d", i)
    iter.Seek([]byte(key))
    if !iter.Valid() {
      t.Errorf("iter seek val %s invalid", key)
      break
    }
    
    if goal := iter.Key().([]byte); string(goal) != key {
      t.Errorf("iter seek val %s / %s not found", key, goal)
    } 
    
    if goal := iter.Value().([]byte); string(goal) != val {
      t.Errorf("iter seek val %s / %s not found", val, goal)
    }
  }  
  defer table.Close()
  
  defer os.Remove(filename)

  if builder.Status() != FINISH {
    t.Errorf("builder status %d after finish", builder.Status())
  }
}
