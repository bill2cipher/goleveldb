package filter

import (
  "bytes"
  "encoding/binary"
)

// This builder is used to construct filter block within table
type BlockBuilder interface {
  // Add another key into this builder
  AddKey(key []byte)

  // Finish building of this block, and returns the FilterBlock built
  Finish() []byte

  // Start another block
  StartBlock(offset int)

  // Clear data within this filter builder
  Reset()

  // Return the current filter block size
  Size() int
}

const (
  base = 2048
  baseLg = 11
)

type filterBuilderImpl struct {
  policy Policy
  keys   [][]byte
  buffer bytes.Buffer
  offset  []uint32
}

func NewBlockBuilder(policy Policy) BlockBuilder {
  builder := new(filterBuilderImpl)
  builder.init(policy)
  builder.keys = [][]byte{}
  return builder
}

func (f *filterBuilderImpl) init(policy Policy) {
  f.policy = policy
}

func (f *filterBuilderImpl) Reset() {
  f.keys = [][]byte{}
  f.buffer.Reset()
}

func (f *filterBuilderImpl) Size() int {
  size := f.buffer.Len()
  size += len(f.offset) * 4 + 5
  return size
}

func (f *filterBuilderImpl) AddKey(key []byte) {
  f.keys = append(f.keys, key)
}

func (f *filterBuilderImpl) StartBlock(offset int) {
  index := offset / base
  for len(f.offset) < index {
    f.generateFilter()
  }
}

func (f *filterBuilderImpl) Finish() []byte {
  if len(f.keys) != 0 {
    f.generateFilter()
  }

  store := make([]byte, 4)
  indexOffset := f.buffer.Len()

  for _, i := range f.offset {
    binary.LittleEndian.PutUint32(store, i)
    f.buffer.Write(store)
  }
  binary.LittleEndian.PutUint32(store, uint32(indexOffset))
  f.buffer.Write(store)

  f.buffer.WriteByte(baseLg)
  return f.buffer.Bytes()
}

func (f *filterBuilderImpl) generateFilter() {
  f.offset = append(f.offset, uint32(f.buffer.Len()))
  if len(f.keys) == 0 {
    return
  }
  rslt := f.policy.CreateFilter(f.keys)
  f.buffer.Write(rslt)
  f.keys = [][]byte{}
}
