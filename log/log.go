package log

const (
  ZeroType  byte = 0
  FullType   = 1
  FirstType  = 2   
  MiddleType = 3
  LastType   = 4
  BadRecord  = 5
)

const (
  MaxRecordType = LastType
  BlockSize = 32768
  HeaderSize = 4 + 2 + 1
)