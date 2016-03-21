package log

import (
  "os"
  "errors"
  "bytes"
  "hash/crc32"
  "encoding/binary"
)

type Reader interface {
  Read() ([]byte, error)
  Close() error
}

func NewReader(filename string, check bool, initOffset uint32) (Reader, error) {
  reader := new(ReaderImpl)
  if err := reader.init(filename, check, initOffset); err != nil {
    return nil, err
  } else {
    return reader, nil
  }
}

type ReaderImpl struct {
  check bool
  file *os.File
  store  []byte
  buffer []byte
  bufferEndOffset uint32

  initOffset   uint32
  lastRecordOffset uint32
}

func (r *ReaderImpl) init(filename string, check bool, initOffset uint32) error {
  if file, err := os.OpenFile(filename, os.O_RDONLY, 0); err != nil {
    return err
  } else {
    r.file = file
    r.initOffset = initOffset
    r.lastRecordOffset = 0
    r.store = make([]byte, BlockSize)
    r.buffer = make([]byte, 0)
    r.bufferEndOffset = 0
    return nil
  }
}

func (r *ReaderImpl) Read() ([]byte, error) {
  if r.lastRecordOffset < r.initOffset {
    if err := r.skipToInitBlock(); err != nil {
      return nil, err
    }
  }

  withinRecord := false
  var rslt bytes.Buffer

  for true {
    rtype, data, err := r.readPartialRecord()
    if err != nil {
      return nil, err
    } else if rtype == BadRecord {
      continue
    }

    if rtype == FullType {
      r.lastRecordOffset = r.bufferEndOffset - uint32(len(r.buffer))
      rslt.Write(data)
      break
    }

    if rtype == FirstType && rslt.Len() == 0 {
      withinRecord = true
      rslt.Write(data)
      continue
    }

    if rtype == MiddleType && withinRecord {
      rslt.Write(data)
      continue
    }

    if rtype == LastType && withinRecord {
      rslt.Write(data)
      withinRecord = false
      break
    } 
  }
  return rslt.Bytes(), nil
}

func (r *ReaderImpl) readPartialRecord() (uint8, []byte, error) {
  if len(r.buffer) < HeaderSize {
    if cnt, err := r.file.Read(r.store); err != nil {
      r.bufferEndOffset += uint32(cnt)
      r.buffer = make([]byte, 0)
      return 0, nil, err
    } else {
      r.buffer = r.store[:cnt]
      r.bufferEndOffset += uint32(cnt)
    }
  }

  checkSum := binary.LittleEndian.Uint32(r.buffer)
  length   := uint32(binary.LittleEndian.Uint16(r.buffer[4:]))
  rtype := r.buffer[HeaderSize - 1]

  if (length + HeaderSize) > uint32(len(r.buffer)) {
    r.buffer = make([]byte, 0)
    return BadRecord, nil, nil
  }

  if rtype == ZeroType && length == 0 {
    r.buffer = make([]byte, 0)
    return BadRecord, nil, nil
  }

  if r.check {
    calcSum := crc32.ChecksumIEEE(r.buffer[HeaderSize:HeaderSize + length])
    if calcSum != checkSum {
      r.buffer = make([]byte, 0)
      return 0, nil, errors.New("crc32 check failed")
    }
  }

  if r.bufferEndOffset - uint32(len(r.buffer)) < r.initOffset {
    r.buffer = r.buffer[HeaderSize + length : ]
    return BadRecord, nil, nil
  }

  rslt := r.buffer[HeaderSize : HeaderSize + length]
  r.buffer = r.buffer[HeaderSize + length:]
  return rtype, rslt, nil
}

func (r *ReaderImpl) skipToInitBlock() error {
  initBlockOffset := r.initOffset - r.initOffset % BlockSize
  if BlockSize - r.initOffset % BlockSize < HeaderSize {
    initBlockOffset += BlockSize
  }

  if initBlockOffset < 0 {
    return errors.New("offset little than 0")
  }

  if _, err := r.file.Seek(int64(initBlockOffset), os.SEEK_SET); err != nil {
    return err
  } else {
    r.bufferEndOffset = initBlockOffset
    return nil
  }
}

func (r *ReaderImpl) Close() error {
  return r.file.Close()
}

func (r *ReaderImpl) LastRecordOffset() uint32 {
  return r.lastRecordOffset
}