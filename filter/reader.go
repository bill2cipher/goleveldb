package filter

import (
  "math"
  "errors"
  "encoding/binary"
)

// Reader parses bytes created with builder.
type BlockReader interface {
  // Return true iff key may exists
  KeyMayMatch(offset uint32, key []byte) bool
}

func NewBlockReader(policy Policy, block []byte) BlockReader {
  reader := new(filterReaderImpl)
  if err := reader.init(policy, block); err != nil {
    return nil
  }
  return reader
}

type filterReaderImpl struct {
  policy Policy
  offset []uint32
  base   uint32
  filter []byte
}

func (f *filterReaderImpl) init(policy Policy, block []byte) error {
  f.policy = policy
  f.filter = block
  
  size := uint32(len(block))
  f.base = uint32(block[size - 1]) & 0xFF
  f.base = uint32(math.Pow(2, float64(f.base)))

  index := binary.LittleEndian.Uint32(block[size - 5 : ])

  for i := index; i < size - 5; i += 4 {
    offset := binary.LittleEndian.Uint32(block[i:])
    f.offset = append(f.offset, offset)
  }
  f.offset = append(f.offset, index)

  if !f.check(index) {
    return errors.New("parse block failed")
  }
  return nil
}

func (f *filterReaderImpl) check(limit uint32) bool {
  for i := 1; i < len(f.offset); i++ {
    if f.offset[i] < f.offset[i - 1] {
      return false
    }

    if f.offset[i] > limit {
      return false
    }
  }
  return true
}

func (f *filterReaderImpl) KeyMayMatch(offset uint32, key []byte) bool {
  index := int(offset / f.base)
  if index >= len(f.offset) - 1 {
    return true
  }

  if filter := f.filter[f.offset[index] : f.offset[index + 1]]; len(filter) == 0 {
    return false
  } else {
    return f.policy.KeyMayMatch(key, filter)
  }
}