package version

import (
  "errors"
  "bytes"
  "fmt"
  "encoding/binary"
)

import (
  "github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/table"
)


type VersionEdit struct {
  LogNumber  int
  FileNumber int
  CmpName    string
  Sequence   uint64
  Files      []*entry
  Pointers   []*entry
  Deletes    []*entry
}

func NewVersionEdit() *VersionEdit {
  edit := new(VersionEdit)
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

func (e *VersionEdit) init() {
  e.Clear()
}

func (e *VersionEdit) Clear() {
  e.LogNumber = -1
  e.FileNumber = -1
  e.CmpName = ""
  e.Sequence = 0
  e.Files = []*entry{}
  e.Pointers = []*entry{}
  e.Deletes = []*entry{}
}

func (e *VersionEdit) SetComparatorName(name string) {
  e.CmpName = name
}

func (e *VersionEdit) SetLogNumber(num int) {
  e.LogNumber = num
}


func (e *VersionEdit) SetNextFile(num int) {
  e.FileNumber = num
}


func (e *VersionEdit) SetLastSequence(seq uint64) {
  e.Sequence = seq
}

func (e *VersionEdit) SetCompactPointer(level int, key *util.InternalKey) {
  e.Pointers = append(e.Pointers, &entry{level, key})
}

func (e *VersionEdit) AddFile(level, file, filesize int, smallest, largest *util.InternalKey) {
  meta := &table.FileMetaData {
    AllowSeek : 0,
    Number    : file,
    FileSize  : filesize,
    Smallest  : *smallest,
    Largest   : *largest,
  }
  e.Files = append(e.Files, &entry{level, meta})
}

func (e *VersionEdit) DeleteFile(level int, file int) {
  entry := &entry {level, file}
  e.Deletes = append(e.Deletes, entry)
}

func (e *VersionEdit) Encode() []byte {
  var buffer bytes.Buffer
  store := make([]byte, 8)
  
  if e.LogNumber != -1 {
    binary.LittleEndian.PutUint32(store, uint32(e.LogNumber))
    buffer.WriteByte(typeLogNumber)
    buffer.Write(store[:4])
  }
  
  if e.FileNumber != -1 {
    binary.LittleEndian.PutUint32(store, uint32(e.FileNumber))
    buffer.WriteByte(typeFileNumber)
    buffer.Write(store[:4])
  }
  
  if e.CmpName != "" {
    buffer.WriteByte(typeCmpName)
    util.PutLenPrefixBytes(&buffer, store, []byte(e.CmpName))
  }
  
  if e.Sequence != 0 {
    binary.LittleEndian.PutUint64(store, e.Sequence)
    buffer.WriteByte(typeSequence)
    buffer.Write(store)
  }
  
  // encode files
  for i := 0; i < len(e.Files); i++ {
    buffer.WriteByte(typeFiles)
    
    binary.LittleEndian.PutUint32(store, uint32(e.Files[i].level))
    buffer.Write(store[:4])
    
    meta := e.Files[i].value.(*table.FileMetaData)
    binary.LittleEndian.PutUint32(store, uint32(meta.FileSize))
    buffer.Write(store[:4])
    
    binary.LittleEndian.PutUint32(store, uint32(meta.Number))
    buffer.Write(store[:4])
    
    util.PutLenPrefixBytes(&buffer, store, meta.Smallest.Encode())
    util.PutLenPrefixBytes(&buffer, store, meta.Largest.Encode())
  }
  
  // encode deleted file
  for i := 0; i < len(e.Deletes); i++ {
    buffer.WriteByte(typeDeletes)
    binary.LittleEndian.PutUint32(store, uint32(e.Deletes[i].level))
    buffer.Write(store[:4])
    
    value := e.Deletes[i].value.(int)
    binary.LittleEndian.PutUint32(store, uint32(value))
    buffer.Write(store[:4])
  }

  // encode Pointers
  for i := 0; i < len(e.Pointers); i++ {
    buffer.WriteByte(typePointers)
    binary.LittleEndian.PutUint32(store, uint32(e.Pointers[i].level))
    buffer.Write(store[:4])
    
    value := e.Pointers[i].value.(*util.InternalKey)
    util.PutLenPrefixBytes(&buffer, store, value.Encode())
  }
  return buffer.Bytes()
}

func (e *VersionEdit) Decode(data []byte) error {
  var val []byte
  for len(data) > 0 {
    switch data[0] {
    case typeLogNumber:
      e.LogNumber = int(binary.LittleEndian.Uint32(data[1:]))
      data = data[5:]
    case typeFileNumber:
      e.FileNumber = int(binary.LittleEndian.Uint32(data[1:]))
      data = data[5:]
    case typeCmpName:
      var cmpName []byte
      cmpName, data = util.GetLenPrefixBytes(data[1:])
      if cmpName == nil || data == nil {
        return errors.New("bad cmp name")
      }
      e.CmpName = string(cmpName)
    case typeSequence:
      e.Sequence = binary.LittleEndian.Uint64(data[1:])
      data = data[9:]
    case typeFiles:

      level := binary.LittleEndian.Uint32(data[1:])
      data = data[5:]
      meta := new(table.FileMetaData)
      meta.AllowSeek = 0
      meta.FileSize = int(binary.LittleEndian.Uint32(data))
      meta.Number = int(binary.LittleEndian.Uint32(data[4:]))
      data = data[8:]
      val, data = util.GetLenPrefixBytes(data)
      if val == nil || data == nil {
        return errors.New("bad internal key")
      }
      (&meta.Smallest).Decode(val)
      
      val, data = util.GetLenPrefixBytes(data)
      if val == nil || data == nil {
        return errors.New("bad internal key")
      }
      (&meta.Largest).Decode(val)
      
    e.Files = append(e.Files, &entry{int(level), meta})
    case typeDeletes:
      level := binary.LittleEndian.Uint32(data[1:])
      num   := binary.LittleEndian.Uint32(data[5:])
      e.Deletes = append(e.Deletes, &entry{int(level), int(num)})
      data = data[9:]

    case typePointers:
      level := binary.LittleEndian.Uint32(data[1:])
      val, data = util.GetLenPrefixBytes(data[5:])
      if val == nil || data == nil {
        return errors.New("bad Pointers key")
      }
      intern := new(util.InternalKey)
      intern.Decode(val)
      e.Pointers = append(e.Pointers, &entry{int(level), intern})
      
    default:
      msg := fmt.Sprintf("bad edit type %d", data[0])
      return errors.New(msg)
    }
  }
  return nil
}
