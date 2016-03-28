package table

import (
  "os"
  "fmt"
  "encoding/binary"
  "hash/crc32"
  "errors"
)

import (
  "github.com/jellybean4/goleveldb/filter"
  "github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/mem"
)

type FileMetaData struct {
  AllowSeek int
  Number    int
  FileSize  int
  Smallest  util.InternalKey
  Largest   util.InternalKey
}


// TableBuilder provides the interface used to build a Table
// (an immutable and sorted map from keys to values).
//
// Multiple threads can invoke const methods on a TableBuilder without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same TableBuilder must use
// external synchronization.
type TableBuilder interface {

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  Add(key, value []byte) error

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  Flush() error

  // Return non-ok iff some error has been detected.
  Status() int

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  Finish() error

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  Abandon()

  // Number of calls to Add() so far.
  NumEntries() int

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  FileSize() int
}

const (
  FINISH = iota
  ABANDON
  ERROR
  OK
)

type tableBuilderImpl struct {
  blockBuilder BlockBuilder
  idxBuilder   BlockBuilder
  option       *util.Option
  file         *os.File
  metaindex    []entry
  filterBuilder filter.BlockBuilder
  offset       int
  status       int
  entries      int
  lastKey      []byte
}

func NewTableBuilder(filename string, option *util.Option) TableBuilder {
  builder := new(tableBuilderImpl)
  if builder.init(filename, option) != nil {
    return nil
  }
  return builder
}

func (t *tableBuilderImpl) init(filename string, option *util.Option) error {
  if file, err := os.OpenFile(filename, os.O_TRUNC | os.O_WRONLY | os.O_CREATE, 0600); err != nil {
    return err
  } else {
    t.blockBuilder = NewBlockBuilder(option.Interval)
    t.idxBuilder = NewBlockBuilder(1)
    t.option = option
    t.file = file
    t.metaindex = []entry{}
    t.filterBuilder = nil
    t.offset = 0
    t.entries = 0
    t.status = OK
    t.lastKey = []byte{}

    if option.Policy != nil {
      t.filterBuilder = filter.NewBlockBuilder(option.Policy)
    } 
    return nil
  }
}

func (t *tableBuilderImpl) Status() int {
  return t.status
}

func (t *tableBuilderImpl) Abandon() {
  filename := t.file.Name()
  t.file.Close()
  t.status = ABANDON
  os.Remove(filename)
}

func (t *tableBuilderImpl) NumEntries() int {
  return t.entries
}

func (t *tableBuilderImpl) FileSize() int {
  return t.offset
}

func (t *tableBuilderImpl) Flush() error {
  if t.status != OK || t.blockBuilder.Empty() {
    return nil
  }
  return t.addBlock(t.lastKey)
}

func (t *tableBuilderImpl) Finish() error {

  if t.blockBuilder.Empty() {
  } else if err := t.addBlock(t.lastKey); err != nil {
    t.status = ERROR
    return err
  }
  defer t.file.Close()

  metaBuilder := NewBlockBuilder(1)
  if t.filterBuilder != nil {
    filter := t.filterBuilder.Finish()
    metaHandler := &BlockHandler{len(filter), t.offset}
    metaName := fmt.Sprintf("filter.%s", t.option.Policy.Name())
    metaBuilder.Add([]byte(metaName), EncodeHandler(metaHandler))   
    if _, err := t.file.Write(filter); err != nil {
      t.status = ERROR
      return err
    }
    t.offset += len(filter)
  }
  
  metaidxBlock := metaBuilder.Finish()
  if _, err := t.file.Write(metaidxBlock); err != nil {
    t.status = ERROR
    return err
  }

  footer := new(FooterHandler)
  footer.metaindex = &BlockHandler{len(metaidxBlock), t.offset}
  t.offset += len(metaidxBlock)

  idxBlock := t.idxBuilder.Finish()
  if _, err := t.file.Write(idxBlock); err != nil {
    t.status = ERROR
    return err
  }

  footer.index = &BlockHandler{len(idxBlock), t.offset}
  t.offset += len(idxBlock)

  if _, err := t.file.Write(footer.Encode()); err != nil {
    t.status = ERROR
    return err
  }
  t.offset += footer.Size()
  
  t.status = FINISH
  return nil
}

func (t *tableBuilderImpl) Add(key, value []byte) error {
  if t.status != OK {
    return errors.New("block status failed")
  }
  if t.blockBuilder.CurrentSizeEstimate() >= t.option.BlockSize {
    t.addBlock(key)
  }

  t.blockBuilder.Add(key, value)
  if t.filterBuilder != nil {
    t.filterBuilder.AddKey(key)
  }
  t.entries++
  t.lastKey = key
  return nil
}

func (t *tableBuilderImpl) addBlock(successor []byte) error {
  sep := t.option.Comparator.FindShortestSep(t.lastKey, successor).([]byte)
  block := t.blockBuilder.Finish()
  t.blockBuilder.Reset()

  handler := &BlockHandler{len(block), t.offset}
  t.idxBuilder.Add(sep, EncodeHandler(handler))
  
  if t.filterBuilder != nil {
    t.filterBuilder.StartBlock(t.offset)
  }

  if cnt, err := t.file.Write(block); err != nil {
    t.status = ERROR
    return err
  } else if cnt != len(block) {
    t.status = ERROR
    msg := fmt.Sprintf("write block %d/%d", cnt, len(block))
    return errors.New(msg)
  }
  t.offset += len(block)
  
  trailer := t.blockTrailer(block)
  if cnt, err := t.file.Write(trailer); err != nil {
    t.status = ERROR
    return err
  } else if cnt != len(trailer) {
    t.status = ERROR
    msg := fmt.Sprintf("write block trailer %d/%d", cnt, len(trailer))
    return errors.New(msg)
  }
  t.offset += len(trailer)
  return nil
}

func (t *tableBuilderImpl) blockTrailer(content []byte) []byte {
  store := make([]byte, 5)
  store[0] = 0
  crc := crc32.ChecksumIEEE(content)
  binary.LittleEndian.PutUint32(store[1:], crc)
  return store
}


// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
type Table interface {
  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  NewIterator() mem.Iterator
  
  // Close the given table
  Close()

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  ApproximateOffsetOf(key []byte) int
  
  // Get the given key/value pair from table if there's any
  Get(key []byte) ([]byte, []byte)
}

type tableImpl struct {
  index     Block
  metaindex Block
  footer    *FooterHandler
  filter    filter.BlockReader
  file      *os.File
  filesize  int
  option    *util.Option
}

// OpenTable attempt to open the table that is stored in bytes [0..file_size)
// of "file", and read the metadata entries necessary to allow
// retrieving data from the table.
//
// If successful, returns ok and sets "*table" to the newly opened
// table.  The client should delete "*table" when no longer needed.
// If there was an error while initializing the table, sets "*table"
// to NULL and returns a non-ok status.  Does not take ownership of
// "*source", but the client must ensure that "source" remains live
// for the duration of the returned table's lifetime.
//
// *file must remain live while this Table is in use.
func OpenTable(filename string, filesize int, option *util.Option) Table {
  table := new(tableImpl)
  if err := table.init(filename, filesize, option); err != nil {
    return nil
  }
  return table
}

func (t *tableImpl) init(filename string, filesize int, option *util.Option) error {
  if file, err := os.OpenFile(filename,os.O_RDONLY, 0); err != nil {
    return err
  } else {
    t.file = file
    t.option = option
    t.filesize = filesize
    return t.parseTable()
  }
}

func (t *tableImpl) Close() {
  t.file.Close()
}

func (t *tableImpl) Get(key []byte) ([]byte, []byte) {
  iiter := t.index.NewIterator(util.BinaryComparator)
  iiter.Seek(key)
  if !iiter.Valid() {
    return nil, nil
  }
  handler := DecodeHandler(iiter.Value().([]byte))
  if t.filter != nil && !t.filter.KeyMayMatch(uint32(handler.offset), key) {
    return nil, nil
  }
  
  if biter := t.NewBlockIterator(iiter.Value()); biter == nil {
    return nil, nil
  } else if biter.Seek(key); !biter.Valid() {
    return nil, nil
  } else {
    return biter.Key().([]byte), biter.Value().([]byte)
  }
}

func (t *tableImpl) parseTable() error {
  if err := t.parseFooter(); err != nil {
    return err
  }
  
  if idxBlock, err := t.parseIndex(t.footer.index); err != nil {
    return err
  } else {
    t.index = idxBlock
  }

  if metaBlock, err := t.parseIndex(t.footer.metaindex); err != nil {
    return err
  } else {
    t.metaindex = metaBlock
  }
  
  if err := t.parseFilter(); err != nil {
    return err
  }
  return nil
}

func (t *tableImpl) parseFilter() error {
  t.filter = nil

  if t.option.Policy == nil {
    return nil
  }

  filtername := fmt.Sprintf("filter.%s", t.option.Policy.Name())
  iter := t.metaindex.NewIterator(t.option.Comparator)
  iter.Seek([]byte(filtername))
  
  if !iter.Valid() {
    return errors.New("could not find given policy")   
  } else if name := iter.Key().([]byte); string(name) != filtername {
    return errors.New("could not find given policy")
  }
  
  handler := DecodeHandler(iter.Value().([]byte))
  if content, err :=  t.readContent(handler.offset, handler.size); err != nil {
    return err
  } else {
    t.filter = filter.NewBlockReader(t.option.Policy, content)
  }
  return nil
}

func (t *tableImpl) parseIndex(handler *BlockHandler) (Block, error) {
  if handler.size > t.filesize {
    msg := fmt.Sprintf("index size too large %d/%d", handler.size, t.filesize)
    return nil, errors.New(msg)
  }

  content, err := t.readContent(handler.offset, handler.size)
  if err != nil {
    return nil, err
  }
  reader := NewBlock(content)
  if reader == nil {
    return nil, errors.New("parse index block failed")
  }

  return reader, nil
}

func (t *tableImpl) parseFooter() error {
  footer := new(FooterHandler)
  content, err := t.readContent(t.filesize - footer.Size(), footer.Size())
  if err != nil {
    return err
  }
  if !footer.Decode(content) {
    return errors.New("decoder footer failed")
  }
  t.footer = footer
  return nil
}

func (t *tableImpl) readContent(offset, size int) ([]byte, error) {
  buffer := make([]byte, size)
  t.file.Seek(int64(offset), os.SEEK_SET)

  if cnt, err := t.file.Read(buffer); err != nil {
    return nil, err
  } else if cnt < size {
    msg := fmt.Sprintf("read content failed %d / %d", cnt, size)
    return nil, errors.New(msg)
  }
  return buffer, nil
}


func (t *tableImpl) NewIterator() mem.Iterator {
  indexIter := t.index.NewIterator(t.option.Comparator)
  return NewTwoLevelIterator(indexIter, t.NewBlockIterator, &util.DefaultReadOption,
      util.BinaryCompare)
}

func (t *tableImpl) ApproximateOffsetOf(key []byte) int {
  iter := t.index.NewIterator(t.option.Comparator)
  iter.Seek(key)
  if !iter.Valid() {
    return t.filesize
  }
  
  handler := DecodeHandler(iter.Value().([]byte))
  return handler.offset
}

func (t *tableImpl) NewBlockIterator(value interface{}) mem.Iterator {
  handler := DecodeHandler(value.([]byte))
  if content, err := t.readContent(handler.offset, handler.size); err != nil {
    return nil
  } else if block := NewBlock(content); block == nil {
    return nil
  } else {
      return block.NewIterator(t.option.Comparator)
  }
}
