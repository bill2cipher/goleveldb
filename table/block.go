package table

import (
  "bytes"
  "errors"
  "encoding/binary"
)

import (
  "github.com/goleveldb/util"
  "github.com/goleveldb/mem"
)

// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.
type BlockBuilder interface {

  // Reset all data within the builder
  Reset()

  // Finish the current block build
  Finish() []byte

  // Add another pair into the block
  Add(key, value []byte)

  // Returns an estimate of the current size of the block
  CurrentSizeEstimate() int

  // Return true iff no entries have been added
  Empty() bool
}

type blockBuilderImpl struct {
  restart  []int
  entryCnt int
  bytes    int
  lastKey  []byte
  interval int
  buffer   bytes.Buffer
}

func NewBlockBuilder(interval int) BlockBuilder {
  builder := new(blockBuilderImpl)
  builder.init(interval)
  return builder
}

func (b *blockBuilderImpl) init(interval int) {
  b.interval = interval
  b.entryCnt = 0
  b.bytes    = 0
  b.restart = []int{0}
  b.lastKey = []byte{}
}

func (b *blockBuilderImpl) Reset() {
  b.entryCnt = 0
  b.bytes    = 0
  b.restart = []int{0}
  b.lastKey = []byte{}
  b.buffer.Reset()
}

func (b *blockBuilderImpl) Finish() []byte {
  store := make([]byte, 4)
  for _, p := range b.restart {
    binary.LittleEndian.PutUint32(store, uint32(p))
    b.buffer.Write(store)
  }
  binary.LittleEndian.PutUint32(store, uint32(len(b.restart)))
  b.buffer.Write(store)
  return b.buffer.Bytes()
}

func (b *blockBuilderImpl) CurrentSizeEstimate() int {
  return b.bytes + 4 * len(b.restart) + 4
}

func (b *blockBuilderImpl) Empty() bool {
  return b.buffer.Len() == 0
}

func (b *blockBuilderImpl) Add(key, value []byte) {
  if b.entryCnt == b.interval {
    b.entryCnt = 0
    b.lastKey = []byte{}
    b.restart = append(b.restart, b.bytes)
  }
  
  b.lastKey, key = key, b.cutCommonPrefix(key)
  header := make([]byte, 3 * 4)
  binary.LittleEndian.PutUint32(header, uint32(len(b.lastKey) - len(key)))
  binary.LittleEndian.PutUint32(header[4:], uint32(len(key)))
  binary.LittleEndian.PutUint32(header[8:], uint32(len(value)))
  b.buffer.Write(header)
  b.buffer.Write(key)
  b.buffer.Write(value)
  b.bytes += 3 * 4 + len(key) + len(value)
  b.entryCnt++
}

func (b *blockBuilderImpl) cutCommonPrefix(key []byte) []byte {
  var minLen int
  if len(key) < len(b.lastKey) {
    minLen = len(key)
  } else {
    minLen = len(b.lastKey)
  }

  var pos int = 0
  for pos = 0; pos < minLen; pos++ {
    if key[pos] != b.lastKey[pos] {
      break
    }
  }
  return key[pos:]
}

type Block interface {
  // size of the block
  Size() int

  // constructs a new iterator for this block
  NewIterator(cmp util.Comparator) mem.Iterator
}

type blockImpl struct {
  content []byte
  restart []int
  limit   int
}

func NewBlock(content []byte) Block {
  block := new(blockImpl)
  if err := block.init(content); err != nil {
    return nil
  }
  return block
}

func (b *blockImpl) init(content []byte) error {
  b.content = content

  size := len(content)
  cnt := int(binary.LittleEndian.Uint32(content[size - 4 : ]))

  if size < (cnt + 1) * 4 {
    return errors.New("content too small to be decode")
  }

  b.limit = size - (cnt + 1) * 4
  for i := 0; i < cnt; i++ {
    offset := b.limit + i * 4
    point := int(binary.LittleEndian.Uint32(content[offset : ]))
    b.restart = append(b.restart, point)
  }

  for _, point := range b.restart {
    if point > b.limit {
      return errors.New("content too small to be decode")
    }
  }
  return nil
}

func (b *blockImpl) Size() int {
  return len(b.content)
}

func (b *blockImpl) NewIterator(cmp util.Comparator) mem.Iterator {
  return newBlockIterator(b, cmp)
}

type blockIterImpl struct {
  offset     int
  entry      *blockEntry
  block      *blockImpl
  cmp        util.Comparator
}

type blockEntry struct {
  shared   int
  unshared int
  partial  []byte
  value    []byte
  key      []byte
}

func (b *blockEntry) size() int {
  if b.partial == nil || b.value == nil {
    return 0
  }
  return 4 * 3 + len(b.partial) + len(b.value)
}

func newBlockIterator(b *blockImpl, cmp util.Comparator) mem.Iterator {
  iter := new(blockIterImpl)
  iter.init(b, cmp)
  return iter
}

func (b *blockIterImpl) init(block *blockImpl, cmp util.Comparator) {
  b.offset = -1
  b.entry = nil
  b.block = block
  b.cmp = cmp
}

func (b *blockIterImpl) Valid() bool {
  return b.entry != nil
}

func (b *blockIterImpl) Key() interface{} {
  if !b.Valid() {
    return nil
  }
  return b.entry.key
}

func (b *blockIterImpl) Value() interface{} {
  if !b.Valid() {
    return nil
  }
  return b.entry.value
}

func (b *blockIterImpl) Next() {
  if !b.Valid() {
    return
  }
  b.decodeNextEntry(b.entry.key)
}

func (b *blockIterImpl) Prev() {
  if !b.Valid() {
    return
  }

  var offset, limit int = -1, b.offset
  for _, point := range b.block.restart {
    if point < b.offset {
      offset = point
    } else {
      break
    }
  }
  
  if offset == -1 {
    b.entry = nil
    return
  }

  b.offset, b.entry = offset, nil
  last := []byte{}
  for b.entry == nil || b.offset + b.entry.size() < limit {
    if err := b.decodeNextEntry(last); err != nil {
      return
    }
    last = b.entry.key
  }
}

func (b *blockIterImpl) Seek(key interface{}) {
  skey := key.([]byte)
  entry := new(blockEntry)
  left, right, mid := 0, len(b.block.restart) - 1, 0

  for left < right {
    mid = (left + right + 1) / 2
    offset := b.block.restart[mid]
    if err := b.decodeEntry(offset, entry, []byte{}); err != nil {
      return
    }

    if cmp := b.cmp.Compare(skey, entry.key); cmp < 0 {
      right = mid - 1
    } else if cmp >= 0 {
      left = mid
    }
  }
  
  b.offset, b.entry = b.block.restart[left], nil
  last := []byte{}
  for true {
    if err := b.decodeNextEntry(last); err != nil {
      return
    }

    if cmp := b.cmp.Compare(skey, b.entry.key); cmp <= 0 {
      return
    }
    
    last = b.entry.key
  }
}

func (b *blockIterImpl) SeekToFirst() {
  b.offset, b.entry = 0, nil
  b.decodeNextEntry([]byte{})
}

func (b *blockIterImpl) SeekToLast() {
  b.offset, b.entry = b.block.restart[len(b.block.restart) - 1], nil
  last := []byte{}
  
  for b.decodeNextEntry(last) == nil && b.nextEntryOffset() < b.block.limit {
    last = b.entry.key
  }
}

func (b *blockIterImpl) nextEntryOffset() int {
  if b.entry == nil {
    return b.offset
  }
  return b.offset + b.entry.size()
}

func (b *blockIterImpl) decodeEntry(offset int, entry *blockEntry, last []byte) error {
  data := b.block.content[offset:]
  entry.shared = int(binary.LittleEndian.Uint32(data))
  data = data[4:]

  entry.unshared = int(binary.LittleEndian.Uint32(data))
  data = data[4:]

  valLen := int(binary.LittleEndian.Uint32(data))
  data = data[4:]

  entry.partial =  data[:entry.unshared]
  data = data[entry.unshared:]

  entry.value = data[: valLen]

  if last == nil || len(last) < entry.shared {
    b.entry = nil
    return errors.New("bad block format")
  }
  store := make([]byte, entry.shared + len(entry.partial))
  for i := 0; i < entry.shared; i++ {
    store[i] = last[i]
  }

  for i := 0; i < len(entry.partial); i++ {
    store[i + entry.shared] = entry.partial[i]
  }
  entry.key = store
  return nil
}

func (b *blockIterImpl) decodeNextEntry(last []byte) error {
  b.offset = b.nextEntryOffset()
  b.entry  = nil
  
  if b.offset >= b.block.limit {
    return errors.New("out of block range")
  }
  
  entry := new(blockEntry)
  if err := b.decodeEntry(b.offset, entry, last); err != nil {
    return err
  }
  
  b.entry = entry
  return nil
}
