package db

import (
  "fmt"
  "bytes"
  "testing"
)

import (
  "github.com/jellybean4/goleveldb/mem"
  "github.com/jellybean4/goleveldb/util"
)

func PrintBatch(t *testing.T, batch WriteBatch) []byte {
  cmp := mem.NewInternalKeyComparator(util.BinaryComparator)
  table := mem.NewMemtable(cmp)
  if err := batch.InsertInto(table); err != nil {
    t.Errorf("batch iterate error %v", err)
    return nil
  }
  cnt := 0
  iter := table.NewIterator()
  var buffer bytes.Buffer 
  
  ikey := new(util.ParsedInternalKey)
  for iter.SeekToFirst(); iter.Valid(); iter.Next() {
    ikey.Decode(iter.Key().([]byte))
    if ikey.Rtype == mem.DeleteType {
      msg := fmt.Sprintf("Delete(%s)", ikey.Key)
      buffer.WriteString(msg)
      cnt++
    } else if ikey.Rtype == mem.ValueType {
      msg := fmt.Sprintf("Put(%s, %s)", ikey.Key, iter.Value().([]byte))
      buffer.WriteString(msg)
      cnt++
    } else {
      t.Errorf("deocde unknown %d", ikey.Rtype)
      return nil
    }
    buffer.WriteString("@")
    buffer.WriteString(fmt.Sprintf("%d", ikey.Seq))
  }
  
  if cnt != batch.Count() {
    t.Errorf("count mismatch %d / %d %s", cnt, batch.Count(), buffer.Bytes())
  }
  return buffer.Bytes()
}

func TestEmpty(t *testing.T) {
  batch := NewWriteBatch()
  if string(PrintBatch(t, batch)) != "" {
    t.Errorf("empty batch not empty")
  }
  
  if batch.Count() != 0 {
    t.Errorf("empty batch count no zero")
  }
}

func TestMulti(t *testing.T) {
  batch := NewWriteBatch()
  batch.Put([]byte("foo"), []byte("bar"))
  batch.Delete([]byte("box"))
  batch.Put([]byte("baz"), []byte("boo"))
  batch.SetSequence(100)
  
  if batch.Sequence() != 100 {
    t.Errorf("batch seq not match")
  }
  
  if batch.Count() != 3 {
    t.Errorf("batch count not match %d", batch.Count)
  }
 
  content := PrintBatch(t, batch)
  if string(content) != "Put(baz, boo)@102Delete(box)@101Put(foo, bar)@100" {
    t.Errorf("batch content not match %s", content)
  }
}

func TestBatch(t *testing.T) {
  b1, b2 := NewWriteBatch(), NewWriteBatch()
  b1.SetSequence(200)
  b2.SetSequence(300)
  b1.Append(b2)
  
  if string(PrintBatch(t, b1)) != "" {
    t.Errorf("error batch content")
  }
  b2.Put([]byte("a"), []byte("va"))
  b1.Append(b2)
  
  if string(PrintBatch(t, b1)) != "Put(a, va)@200" {
    t.Errorf("error append content")
  }
  
  b2.Clear()
  b2.Put([]byte("b"), []byte("vb"))
  b1.Append(b2)
  
  if string(PrintBatch(t, b1)) != "Put(a, va)@200Put(b, vb)@201" {
    t.Errorf("error reset content")
  }
  
  b2.Delete([]byte("foo"))
  b1.Append(b2)
  
  if msg := string(PrintBatch(t, b1)); msg != "Put(a, va)@200Put(b, vb)@202Put(b, vb)@201Delete(foo)@203" {
    t.Errorf("final concat error %s", msg)
  }
}