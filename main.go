package main

import (
  "fmt"
  "encoding/binary"
  "hash/crc64"
  "bytes"
)

func key(v uint64) uint64 {
  return v >> 40
}

func gen(v uint64) uint64 {
  return v >> 8 & 0xFFFFFFFF
}

func hash(v uint64) uint64 {
  return v & 0xFF
}

func mkey(k, g uint64) uint64 {
  return (k << 40 | g << 8) | (hashNumber(k, g) & 0xFF)
}

func hashNumber(k, g uint64) uint64 {
  buffer := make([]byte, 16)
  binary.LittleEndian.PutUint64(buffer, k)
  binary.LittleEndian.PutUint64(buffer[8:], g)
  table := crc64.MakeTable(crc64.ECMA)
  h := crc64.New(table)
  h.Write(buffer)
  rslt := h.Sum64()
  h.Reset()
  return rslt
}

func main() {
  var v uint64 = 1099511645686
  k := key(v)
  g := gen(v)
  fmt.Printf("%d %d %d %d %d %d\n", v, k, g, hash(v), mkey(k, g), hashNumber(k, g) & 0xFF)
  var buffer bytes.Buffer
  buffer.Write([]byte{1,2,3,4,5,6,7})
  
  store := buffer.Bytes()
  store[2] = 12
  
  fmt.Printf("%v %v", buffer.Bytes(), store)
  
  a := []int{}
  fmt.Printf("%d", len(a))
}
