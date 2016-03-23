package util

import (
  "sort"
  "bytes"
  "errors"
  "encoding/binary"
)

type LookupKey struct {
  key   []byte
  seq   uint64
  rtype byte
}

func NewLookupKey(key []byte, seq uint64, rtype byte) *LookupKey {
  lookup := new(LookupKey)
  lookup.key= key
  lookup.seq = seq
  lookup.rtype = rtype
  return lookup
}

func (l *LookupKey) MemtableKey() []byte {
  buffer := make([]byte, 4 + 8 + len(l.key))
  binary.LittleEndian.PutUint32(buffer, uint32(8 + len(l.key)))
  for i := 0; i < len(l.key); i++ {
    buffer[4 + i] = l.key[i]
  }
  binary.LittleEndian.PutUint64(buffer[4 + len(l.key):], PackSeqAndType(l.seq, l.rtype))
  return buffer
}

func (l *LookupKey) UserKey() []byte {
  return l.key
}

func (l *LookupKey) InternalKey() []byte {
  store := make([]byte, len(l.key) + 8)
  for i, b := range l.key {
    store[i] = b
  }
  binary.LittleEndian.PutUint64(store[len(l.key):], PackSeqAndType(l.seq, l.rtype))
  return store
}

type ParsedInternalKey struct {
  Key   []byte
  Seq   uint64
  Rtype byte
}

func (p *ParsedInternalKey) Decode(data []byte) error {
  if len(data) < 8 {
    return errors.New("bad internal key format")
  }
  p.Key = data[ : len(data) - 8]
  p.Seq = binary.LittleEndian.Uint64(data[len(data) - 8 : ])
  p.Rtype = byte(p.Seq & 0xFF)
  p.Seq = p.Seq >> 8
  return nil
}

func (p *ParsedInternalKey) Encode() []byte {
  buffer := make([]byte, 12 + len(p.Key))
  rslt := buffer

  binary.LittleEndian.PutUint32(buffer, uint32(12 + len(p.Key)))
  buffer = buffer[4:]
  for i := 0; i < len(p.Key); i++ {
    buffer[i] = p.Key[i]
  }
  buffer = buffer[len(p.Key):]
  binary.LittleEndian.PutUint64(buffer, PackSeqAndType(p.Seq, p.Rtype))
  return rslt
}

type InternalKey struct {
  content []byte
}

func NewInternalKey(key []byte, seq uint64, rtype byte) *InternalKey {
  store := make([]byte, len(key) + 8)
  data  := store
  for i := 0; i < len(key); i++ {
    data[i] = key[i]
  }
  data = data[len(key):]
  binary.LittleEndian.PutUint64(data, PackSeqAndType(seq, rtype))
  
  internal := new(InternalKey)
  internal.content = store
  return internal
}

func (i *InternalKey) Encode() []byte {
  return i.content
}

func (i *InternalKey) Decode(data []byte) {
  i.content = data
}

func (i *InternalKey) UserKey() []byte {
  return i.content[ : len(i.content) - 8]
}

func (i *InternalKey) Clear() {
  i.content = nil
}

func (i *InternalKey) SetFrom(p *ParsedInternalKey) {
  i.content, _ = GetLenPrefixBytes(p.Encode())
}


func GetLenPrefixBytes(data []byte) ([]byte, []byte) {
  if len(data) < 4 {
    return nil, nil
  }

  clen := binary.LittleEndian.Uint32(data)
  if uint32(len(data)) < (4 + clen) {
    return nil, nil
  }

  return data[4 : clen + 4], data[clen + 4 : ]
}

func PutLenPrefixBytes(buffer *bytes.Buffer, store, data []byte) {
  slen := uint32(len(data))
  binary.LittleEndian.PutUint32(store, slen)
  buffer.Write(store[:4])
  buffer.Write(data)
}

func GetBytesLen(data []byte) []byte {
  store := make([]byte, 4)
  binary.LittleEndian.PutUint32(store, uint32(len(data)))
  return store
}

func PackSeqAndType(seq uint64, rtype byte) uint64 {
  return seq << 8 | (uint64(rtype) & 0xFF)
}

// ExtractUserKey extracts user key from internal key
func ExtractUserKey(ikey []byte) []byte {
  clen := len(ikey)
  return ikey[:clen - 8]
}

type Less func(f, s interface{}) bool
type SliceSorter struct {
  less Less
  data []interface{}
}

func NewSliceSorter(data []interface{}, less Less) sort.Interface {
  sorter := new(SliceSorter)
  sorter.less = less
  sorter.data = data
  return sorter
}

func (s *SliceSorter) Len() int {
  return len(s.data)
}

func (s *SliceSorter) Swap(i, j int) {
  s.data[i], s.data[j] = s.data[j], s.data[i]
}

func (s *SliceSorter) Less(i, j int) bool {
  return s.less(s.data[i], s.data[j])
}

