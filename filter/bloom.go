package filter

import (
  "hash/crc32"
)

// This struct implements the Bloom Filter Policy
type bloomPolicy struct {
  bitsPerKey int
  k          int
}

func NewBloomPolicy(bitsPerKey int) Policy {
  bloom := new(bloomPolicy)
  bloom.init(bitsPerKey)
  return bloom
}

func (b *bloomPolicy) init(bitsPerKey int) {
  b.bitsPerKey = bitsPerKey
  k := int(float32(bitsPerKey) * 0.69)
  if k < 1 {
    k = 1
  } else if k > 30 {
    k = 30
  }
  b.k = k
}

func (b *bloomPolicy) Name() string {
  return "leveldb.BloomFilter"
}

func (b *bloomPolicy) CreateFilter(keys [][]byte) []byte {
  bits := len(keys) * b.bitsPerKey
  bytes := (bits  + 7) / 8
  bits = bytes * 8
  rslt := make([]byte, bytes + 1)
  rslt[bytes] = byte(b.k)
  for _, key := range keys {
    h := crc32.ChecksumIEEE(key)
    var delta uint32 = h >> 17 | h << 15
    for j := 0; j < b.k; j++ {
      bitpos := h % uint32(bits)
      rslt[bitpos / 8] |= 1 << (bitpos % 8)
      h += delta
    }
  }
  return rslt
}


func (b *bloomPolicy) KeyMayMatch(key []byte, filter []byte) bool {
  bytes := len(filter) - 1
  bits  := bytes * 8
  k := int(filter[bytes])
  if k > 30 {
    return true
  }

  h := crc32.ChecksumIEEE(key)
  var delta uint32 = h >> 17 | h << 15
  for j := 0; j < k; j++ {
    bitpos := h % uint32(bits)
    if filter[bitpos / 8] & (1 << (bitpos % 8)) == 0 {
      return false
    }
    h += delta
  }
  return true
}