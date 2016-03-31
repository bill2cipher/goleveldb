package db

import (
  "bytes"
  "errors"
	"encoding/binary"
)

import (
  "github.com/jellybean4/goleveldb/mem"
  "github.com/jellybean4/goleveldb/util"
)

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.
type WriteBatch interface {
  
  // Store the mapping "key->value" in the database.
  Put(key []byte, value  []byte)
  // If the database contains a mapping for "key", erase it.  Else do nothing.
  Delete(key []byte)
  // Clear all updates buffered in this batch.
  Clear()
  // Iterate all the key/val pairs in the bach to the given handle in args
  Iterate(handler BatchHandler) error
  
  // Return the number of entries in the batch.
  Count() int
  
  // Set the count for the number of entries in the batch.
  SetCount(n int)

  // Return the sequence number for the start of this batch.
  Sequence() uint64

  // Store the specified number as the sequence number for the start of
  // this batch.
  SetSequence(seq uint64)

  Contents() []byte

  ByteSize() int

  SetContents(contents []byte)

  InsertInto(memtable mem.Memtable) error ;

  Append(dst WriteBatch);
}

type BatchHandler interface {
  // Process the given key/val pair
  Put(key, value []byte)
  
  // Delete data with the given key
  Delete(key []byte)
}

const (
  BatchHeader = 12
)

type handlerImpl struct {
  mem mem.Memtable
  seq uint64
}

func NewBatchHandler(mem mem.Memtable, seq uint64) BatchHandler {
  handler := new(handlerImpl)
  handler.init(mem, seq)
  return handler
}

func (h *handlerImpl) init(mem mem.Memtable, seq uint64) {
  h.mem = mem
  h.seq = seq
}

func (h *handlerImpl) Put(key, value []byte) {
  h.mem.Add(h.seq, mem.ValueType, key, value)
  h.seq++
}

func (h *handlerImpl) Delete(key []byte) {
  h.mem.Add(h.seq, mem.DeleteType, key, []byte{})
  h.seq++
}

type batchImpl struct {
  buffer bytes.Buffer
}

func NewWriteBatch() WriteBatch {
  batch := new(batchImpl)
  batch.init()
  return batch
}

func (b *batchImpl) init() {
  for i := 0; i < BatchHeader; i++ {
    b.buffer.WriteByte(0)
  }
}

func (b *batchImpl) Put(key, value []byte) {
  b.buffer.WriteByte(mem.ValueType)
  
  b.buffer.Write(util.GetBytesLen(key))
  b.buffer.Write(key)
  
  b.buffer.Write(util.GetBytesLen(value))
  b.buffer.Write(value)
  
  b.SetCount(b.Count() + 1)
}

func (b *batchImpl) Delete(key []byte) {
  b.buffer.WriteByte(mem.DeleteType)
  
  b.buffer.Write(util.GetBytesLen(key))
  b.buffer.Write(key)
  
  b.SetCount(b.Count() + 1)
}

func (b *batchImpl) Clear() {
  b.buffer.Reset()
  for i := 0; i < BatchHeader; i++ {
    b.buffer.WriteByte(0)
  }
}

func (b *batchImpl) Iterate(handler BatchHandler) error {
  content := b.buffer.Bytes()
  if len(content) < BatchHeader {
    return errors.New("batch size too small")
  }
  
  var key, val []byte
  content = content[BatchHeader : ]
  cnt := 0
  for len(content) > 0 {
    if content[0] == mem.ValueType {
      key, content = util.GetLenPrefixBytes(content[1:])
      val, content = util.GetLenPrefixBytes(content)
      if key == nil || val == nil {
        return errors.New("bad patch put")
      }
      handler.Put(key, val)
    } else if content[0] == mem.DeleteType {
      key, content = util.GetLenPrefixBytes(content[1:])
      
      if key == nil {
        return errors.New("bad batch del")
      }
      handler.Delete(key)
    } else {
      return errors.New("bad content format")
    }
    cnt++
  }

  if cnt != b.Count() {
    return errors.New("content count not match")
  }
  return nil
}

func (b *batchImpl) Count() int {
  cnt  := b.buffer.Bytes()[8:]
  rslt := binary.LittleEndian.Uint32(cnt)
  return int(rslt)
}


func (b *batchImpl) SetCount(n int) {
  cnt := b.buffer.Bytes()[8:]
  binary.LittleEndian.PutUint32(cnt, uint32(n))
}


func (b *batchImpl) Sequence() uint64 {
  seq := b.buffer.Bytes()
  return binary.LittleEndian.Uint64(seq)
}

func (b *batchImpl) SetSequence(seq uint64) {
  seqBytes := b.buffer.Bytes()
  binary.LittleEndian.PutUint64(seqBytes, seq)
}

func (b *batchImpl) Contents() []byte {
  return b.buffer.Bytes()
}

func (b *batchImpl) ByteSize() int {
  return b.buffer.Len()
}

func (b *batchImpl) SetContents(contents []byte) {
  if len(contents) < BatchHeader {
    return
  } 
  b.buffer.Reset()
  b.buffer.Write(contents)
}

func (b *batchImpl) InsertInto(memtable mem.Memtable) error {
  handler := NewBatchHandler(memtable, b.Sequence())
  return b.Iterate(handler)
}

func (b *batchImpl) Append(src WriteBatch) {
  b.SetCount(b.Count() + src.Count())
  content := src.Contents()
  b.buffer.Write(content[BatchHeader:])
}
