package log

import (
  "os"
  "hash/crc32"
  "encoding/binary"
)

type Writer interface {
  AddRecord(data []byte) error
  Close() error
}

type WriterImpl struct {
  file *os.File
  headBuffer []byte
  blockOffset uint32
}

func NewWriter(filename string) (Writer, error) {
  writer := new(WriterImpl)
  if err := writer.init(filename); err != nil {
    return nil, err
  } else {
    return writer, nil
  }
}

func (w *WriterImpl) init(filename string) error {
  if file, err := os.OpenFile(filename, os.O_APPEND | os.O_WRONLY | os.O_CREATE, 0660); err != nil {
    return err
  } else {
    w.file = file
    w.blockOffset = 0
    w.headBuffer = make([]byte, HeaderSize)
    w.clearHeader()
    return nil
  }
}

func (w *WriterImpl) AddRecord(data []byte) error {
  var left uint32 = uint32(len(data))
  var dataOffset uint32 = 0
  begin := true
  for true {
    if leftOver := BlockSize - w.blockOffset; leftOver < HeaderSize {
      if leftOver > 0 {
        w.file.Write(w.headBuffer[:leftOver])
      }
      w.blockOffset = 0
    }

    length := w.calcLength(left)
    rtype := w.calcType(begin, left, length)
    if _, err := w.emitPhysicalRecord(data, rtype, dataOffset, length); err != nil {
      return err
    }

    w.blockOffset += HeaderSize + length
    left -= length
    dataOffset += length
    begin = false

    if left <= 0 {
      break;
    }
  }
  return nil
}

func (w *WriterImpl) emitPhysicalRecord(data []byte, rtype uint8, dataOffset , length uint32) (uint32, error) {
  writeData := data[dataOffset : dataOffset + length]
  w.writeHeader(writeData, uint16(length), rtype)
  defer w.clearHeader()

  if cnt, err := w.file.Write(w.headBuffer); err != nil {
    return uint32(cnt), err
  } else if cnt, err = w.file.Write(writeData); err != nil {
    w.file.Sync()
    return uint32(cnt), err
  } else {
    return uint32(cnt), nil
  }
}

func (w *WriterImpl) writeHeader(data []byte, length uint16, rtype byte) {
  checkSum := crc32.ChecksumIEEE(data)
  binary.LittleEndian.PutUint32(w.headBuffer, checkSum)
  binary.LittleEndian.PutUint16(w.headBuffer[4:], length)
  w.headBuffer[HeaderSize - 1] = rtype
}

func (w *WriterImpl) clearHeader() {
  for i := 0; i < HeaderSize; i++ {
    w.headBuffer[i] = 0
  }
}

func (w * WriterImpl) calcType(begin bool, left, length uint32) byte {
  end := left == length
  switch true {
  case begin && end:
    return FullType
  case begin:
    return FirstType
  case end:
    return LastType
  default:
    return MiddleType
  }
}

func (w *WriterImpl) calcLength(left uint32) uint32 {
  if left < (BlockSize - w.blockOffset - HeaderSize) {
    return left
  } else {
    return BlockSize - w.blockOffset - HeaderSize
  }
}

func (w *WriterImpl) Close() error {
  return w.file.Close()
}