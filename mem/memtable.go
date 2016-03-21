package mem

import (
  "bytes"
  "encoding/binary"
)

import (
  "github.com/goleveldb/util"
)



func NewMemtable(cmp util.Comparator) Memtable {
  table := new(memImpl)
  table.init(cmp)
  return table
}

type memImpl struct {
  list Skiplist
  cmp  util.Comparator
  written int
}

func (m *memImpl) init(cmp util.Comparator) {
  m.cmp = cmp
  m.list = NewSkiplist(m)
  m.written = 0
}

func (m *memImpl) ApproximateMemoryUsage() int {
  return m.written
}

func (m *memImpl) NewIterator() Iterator {
  iter := new(memIterator)
  iter.init(m.list)
  return iter
}

// Format of an entry is concatenation of:
// key_size   : uint32 of internal_key.size()
// key bytes  : [internal_key.size()]byte
// value_size : len(value)
// value bytes: [value_size]byte
func (m *memImpl) Add(seq uint64, rtype byte, key, value []byte) {
  var buffer bytes.Buffer
  lenBuf := make([]byte, 8)
  binary.LittleEndian.PutUint32(lenBuf, uint32(8 + len(key)))
  buffer.Write(lenBuf[:4])
  buffer.Write(key)

  binary.LittleEndian.PutUint64(lenBuf, seq << 8 | uint64(rtype))
  buffer.Write(lenBuf)

  binary.LittleEndian.PutUint32(lenBuf, uint32(len(value)))
  buffer.Write(lenBuf[:4])
  buffer.Write(value)

  var node []byte = buffer.Bytes()
  m.list.Insert(node)
  m.written += len(node)
}

func (m *memImpl) Get(key util.LookupKey) []byte {
  sKey := key.MemtableKey()
  iter := m.list.NewIterator()
  iter.Seek(sKey)

  if !iter.Valid() {
    return nil
  }

  offset := 0
  entry := iter.Key().([]byte)
  entryKey, _ := util.GetLenPrefixBytes(entry)
  offset = len(entryKey) + 4
  if util.BinaryCompare(entry[4 : offset - 8], key.UserKey()) != 0 {
    return nil
  }

  rtype := binary.LittleEndian.Uint64(entry[offset - 8:]) & 0xFF
  switch rtype {
  case ValueType:
    val, _ := util.GetLenPrefixBytes(entry[offset : ])
    return val
  case DeleteType:
    return nil
  }
  return nil
}

func (m *memImpl) Compare(a, b interface{}) int {
  a1, b1 := a.([]byte), b.([]byte)
  val1, _ := util.GetLenPrefixBytes(a1)
  val2, _ := util.GetLenPrefixBytes(b1)
  return m.cmp.Compare(val1, val2)
} 

func (m *memImpl) DumpData() []MemEntry {
  iter := m.NewIterator()
  iter.SeekToFirst()
  rslt := []MemEntry{}

  for iter.Valid() {
    key := iter.Key().([]byte)
    seq := binary.LittleEndian.Uint64(key[len(key) - 8 :])

    entry := MemEntry{
      Key : key[:len(key) - 8],
      Seq : seq >> 8,
      Rtype : byte(seq & 0xFF),
      Val : iter.Value().([]byte),
    }
    rslt = append(rslt, entry)
    iter.Next()
  }
  return rslt
}

func decodeEntry(data []byte) ([]byte, []byte) {
  key, _ := util.GetLenPrefixBytes(data)
  val, _ := util.GetLenPrefixBytes(data[4 + len(key) : ])
  return key, val
}

type memIterator struct {
  iter Iterator
  buffer bytes.Buffer
}

func (i *memIterator) init(list Skiplist) {
  i.iter = list.NewIterator()
}

func (i *memIterator) Valid() bool {
  return i.iter.Valid()
}

func (i *memIterator) Key() interface{} {
  key := i.iter.Key().([]byte)
  val, _ := util.GetLenPrefixBytes(key)
  return val
}

func (i *memIterator) Value() interface{} {
  _, val := decodeEntry(i.iter.Key().([]byte))
  return val
}

func (i *memIterator) Next() {
  i.iter.Next()
}

func (i *memIterator) Prev() {
  i.iter.Prev()
}

func (i *memIterator) Seek(key interface{}) {
  keyVal := key.([]byte)
  buffer := make([]byte, len(keyVal) + 4)
  binary.LittleEndian.PutUint32(buffer, uint32(len(keyVal)))
  for i := 0; i < len(keyVal); i++ {
    buffer[i + 4] = keyVal[i]
  }
  i.iter.Seek(buffer)
}

func (i *memIterator) SeekToFirst() {
  i.iter.SeekToFirst()
}

func (i *memIterator) SeekToLast() {
  i.iter.SeekToLast()
}
