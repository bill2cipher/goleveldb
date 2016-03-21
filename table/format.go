package table

import (
  "bytes"
  "encoding/binary"
)

type BlockHandler struct {
  size int
  offset int 
}

func EncodeHandler(b *BlockHandler) []byte {
  store := make([]byte, 8)
  binary.LittleEndian.PutUint32(store, uint32(b.size))
  binary.LittleEndian.PutUint32(store[4:], uint32(b.offset))
  return store
}

func DecodeHandler(value []byte) *BlockHandler {
  handler := new(BlockHandler)
  handler.size = int(binary.LittleEndian.Uint32(value))
  handler.offset = int(binary.LittleEndian.Uint32(value[4:]))
  return handler
}

const (
  FOOTER_MAGIC = 0xdb4775248b80fb57
)

type FooterHandler struct {
  metaindex *BlockHandler
  index     *BlockHandler
  magic     uint64
}

func (footer *FooterHandler) Encode() []byte {
  var buffer bytes.Buffer
  buffer.Write(EncodeHandler(footer.metaindex))
  buffer.Write(EncodeHandler(footer.index))

  store := make([]byte, 8)
  binary.LittleEndian.PutUint64(store, FOOTER_MAGIC)
  buffer.Write(store)
  return buffer.Bytes()
}

func (footer *FooterHandler) Decode(value []byte) bool {
  if magic := binary.LittleEndian.Uint64(value[16:]); magic != FOOTER_MAGIC {
    return false
  }
  footer.metaindex = DecodeHandler(value)
  footer.index = DecodeHandler(value[8:])
  footer.magic = FOOTER_MAGIC
  return true
}

func (footer *FooterHandler) Size() int {
  return 24
}

type entry struct {
  key string
  handler BlockHandler
}

