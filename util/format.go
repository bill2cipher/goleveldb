package util

import (
  "bytes"
  "errors"
  "encoding/binary"
)

type LookupKey struct {
  key   []byte
  seq   uint64
  rtype uint64
}

func NewLookupKey(key []byte, seq uint64, rtype byte) *LookupKey {
  lookup := new(LookupKey)
  lookup.key= key
  lookup.seq = seq
  lookup.rtype = uint64(rtype) & 0xFF
  return lookup
}

func (l *LookupKey) MemtableKey() []byte {
  buffer := make([]byte, 4 + 8 + len(l.key))
  binary.LittleEndian.PutUint32(buffer, uint32(8 + len(l.key)))
  for i := 0; i < len(l.key); i++ {
    buffer[4 + i] = l.key[i]
  }
  binary.LittleEndian.PutUint64(buffer[4 + len(l.key):], l.seq << 8 | l.rtype)
  return buffer
}

func (l *LookupKey) UserKey() []byte {
  return l.key
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

func FindShortestSep(key1, key2 []byte) []byte {
  var minLen int
  if len(key1) > len(key2) {
    minLen = len(key2)
  } else {
    minLen = len(key1)
  }

  var pos int
  for pos = 0; pos < minLen; pos++ {
    if key1[pos] + 1 >= key2[pos] {
      continue
    }
  }

  if pos < minLen {
    key1[pos] += 1
    return key1[:pos + 1]
  }
  return key1
}

func PackSeqAndType(seq uint64, rtype byte) uint64 {
  return seq << 8 | (uint64(rtype) & 0xFF)
}
