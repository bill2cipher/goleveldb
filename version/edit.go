package version

import (
  "errors"
  "bytes"
  "encoding/binary"
)

import (
  "github.com/goleveldb/util"
)


type VersionEdit interface {
  // clear all contents of this version edit
  Clear()
  
  SetComparatorName(name string)
  
  SetLogNumber(num int)
  
  SetNextFile(num int)
  
  SetLastSequence(seq uint64)
  
  SetCompactPointer(level int, key *util.InternalKey)

  // Add the specified file at the specified number.
  // REQUIRES: This version has not been saved (see VersionSet::SaveTo)
  // REQUIRES: "smallest" and "largest" are smallest and largest keys in file 
  AddFile(level, file, file_size int, smallest, largest util.InternalKey)
  
  // Delete the specified "file" from the specified "level"
  DeleteFile(level int, file int)
  
  Encode() []byte
  
  Decode(data []byte) error
}

func NewVersionEdit() VersionEdit {
  edit := new(editImpl)
  edit.init()
  return edit  
}

type entry struct {
  level int
  value interface{}
}

const (
  typeLogNumber = iota
  typeFileNumber
  typeCmpName
  typeSequence
  typeFiles
  typePointers
  typeDeletes
)

type editImpl struct {
  logNumber  int
  fileNumber int
  cmpName    []byte
  sequence   uint64
  files      []*entry
  pointers   []*entry
  deletes    []*entry
}

func (e *editImpl) init() {
  e.Clear()
}

func (e *editImpl) Clear() {
  e.logNumber = -1
  e.fileNumber = -1
  e.cmpName = nil
  e.sequence = 0
  e.files = []*entry{}
  e.pointers = []*entry{}
  e.deletes = []*entry{}
}

func (e *editImpl) SetComparatorName(name string) {
  e.cmpName = []byte(name)
}

func (e *editImpl) SetLogNumber(num int) {
  e.logNumber = num
}

func (e *editImpl) SetNextFile(num int) {
  e.fileNumber = num
}

func (e *editImpl) SetLastSequence(seq uint64) {
  e.sequence = seq
}

func (e *editImpl) SetCompactPointer(level int, key *util.InternalKey) {
  e.pointers = append(e.pointers, &entry{level, key})
}

func (e *editImpl) AddFile(level, file, filesize int, smallest, largest util.InternalKey) {
  meta := &FileMetaData {
    AllowSeek : 0,
    Number    : file,
    FileSize  : filesize,
    Smallest  : smallest,
    Largest   : largest,
  }
  e.files = append(e.files, &entry{level, meta})
}

func (e *editImpl) DeleteFile(level int, file int) {
  entry := &entry {level, file}
  e.deletes = append(e.deletes, entry)
}

func (e *editImpl) Encode() []byte {
  var buffer bytes.Buffer
  store := make([]byte, 8)
  
  if e.logNumber != -1 {
    binary.LittleEndian.PutUint32(store, uint32(e.logNumber))
    buffer.WriteByte(typeLogNumber)
    buffer.Write(store[:4])
  }
  
  if e.fileNumber != -1 {
    binary.LittleEndian.PutUint32(store, uint32(e.fileNumber))
    buffer.WriteByte(typeFileNumber)
    buffer.Write(store[:4])
  }
  
  if e.cmpName != nil {
    buffer.WriteByte(typeCmpName)
    util.PutLenPrefixBytes(&buffer, store, e.cmpName)
  }
  
  if e.sequence != 0 {
    binary.LittleEndian.PutUint64(store, e.sequence) 
    buffer.Write(store)
  }
  
  // encode files
  for i := 0; i < len(e.files); i++ {
    buffer.WriteByte(typeFiles)
    
    binary.LittleEndian.PutUint32(store, uint32(e.files[i].level))
    buffer.Write(store[:4])
    
    meta := e.files[i].value.(*FileMetaData)
    binary.LittleEndian.PutUint32(store, uint32(meta.FileSize))
    buffer.Write(store[:4])
    
    binary.LittleEndian.PutUint32(store, uint32(meta.Number))
    buffer.Write(store)
    
    util.PutLenPrefixBytes(&buffer, store, meta.Smallest.Encode())
    util.PutLenPrefixBytes(&buffer, store, meta.Largest.Encode())
  }
  
  // encode deleted file
  for i := 0; i < len(e.deletes); i++ {
    buffer.WriteByte(typeDeletes)
    binary.LittleEndian.PutUint32(store, uint32(e.deletes[i].level))
    buffer.Write(store[:4])
    
    value := e.deletes[i].value.(int)
    binary.LittleEndian.PutUint32(store, uint32(value))
    buffer.Write(store[:4])
  }

  // encode pointers
  for i := 0; i < len(e.pointers); i++ {
    buffer.WriteByte(typePointers)
    
    value := e.pointers[i].value.(*util.InternalKey)
    util.PutLenPrefixBytes(&buffer, store, value.Encode())
  }
  return nil
}

func (e *editImpl) Decode(data []byte) error {
  for len(data) > 0 {
    switch data[0] {
    case typeLogNumber:
      e.logNumber = int(binary.LittleEndian.Uint32(data[1:]))
      data = data[5:]
    case typeFileNumber:
      e.fileNumber = int(binary.LittleEndian.Uint32(data[1:]))
      data = data[5:]
    case typeCmpName:
      e.cmpName, data = util.GetLenPrefixBytes(data[1:])
      data = data[len(e.cmpName) + 1 :]
    case typeSequence:
      e.sequence = binary.LittleEndian.Uint64(data[1:])
      data = data[9:]
    case typeFiles:
      level := binary.LittleEndian.Uint32(data[1:])
      data = data[5:]
      meta := new(FileMetaData)
      meta.AllowSeek = 0
      meta.FileSize = int(binary.LittleEndian.Uint32(data))
      meta.Number = int(binary.LittleEndian.Uint32(data[4:]))
      data = data[8:]
      val, data := util.GetLenPrefixBytes(data)
      (&meta.Smallest).Decode(val)
      
      val, data = util.GetLenPrefixBytes(data)
      (&meta.Largest).Decode(val)
      
      e.files = append(e.files, &entry{int(level), meta})
    case typeDeletes:
      level := binary.LittleEndian.Uint32(data[1:])
      num   := binary.LittleEndian.Uint32(data[5:])
      e.deletes = append(e.deletes, &entry{int(level), int(num)})
      
    case typePointers:
      level := binary.LittleEndian.Uint32(data[1:])
      val, data := util.GetLenPrefixBytes(data[5:])
      intern := new(util.InternalKey)
      intern.Decode(val)
      e.pointers = append(e.pointers, &entry{int(level), intern})
      
    default:
      return errors.New("bad edit format")
    }
  }
  return nil
}
