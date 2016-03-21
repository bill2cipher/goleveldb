package log

import (
  "os"
  "bytes"
  "math/rand"
  "strings"
  "testing"
  "fmt"
)

func BigString(partial string, size int) string {
  var rslt bytes.Buffer
  for rslt.Len() < size {
    rslt.WriteString(partial)
  }
  rslt.Truncate(size)
  return rslt.String()
}

func NumberString(n int) string {
  return fmt.Sprintf("%d", n)
}

func RandomSkewedString(i int, r *rand.Rand) string {
  d := r.Int() % 17
  return BigString(NumberString(i), d)
}

type StringDest struct {
  contents string
}

func (d *StringDest) Close() {
}

func (d *StringDest) Flush() {
}

func (d *StringDest) Sync() {
}

func (d *StringDest) Append(data string) {
  d.contents += data
}

type StringSource struct {
  contents string
  force_error bool
  returned_partial bool
}

func (s *StringSource) init() {
  s.contents = ""
  s.force_error = false
  s.returned_partial = false
}

func (s *StringSource) Read(size int) string {
  if s.returned_partial {
    panic("must not Read() after eof/error")
  }

  if s.force_error {
    s.force_error = false
    s.returned_partial = true
    panic("read error")
  }

  if len(s.contents) < size {
    s.returned_partial = true
    size = len(s.contents)
  }
  rslt := s.contents[:size]
  s.contents = s.contents[size:]
  return rslt
}

func (s *StringSource) Skip(size int) {
  if size > len(s.contents) {
    s.contents = ""
  }
  s.contents = s.contents[size:]
}

type LogTest struct {
  reader  Reader
  writer  Writer
  reading bool
  dest    *StringDest
  source  *StringSource
  assert  *testing.T
  writtenBytes int
}

var (
  initialOffsetRecordSize = []int{10000, 10000, 2 * BlockSize - 1000, 1}
  initialOffsetLastRecordOffsets = []int{0, HeaderSize + 10000, 2 * (HeaderSize + 10000),
    2 * (HeaderSize + 10000) + (2 * BlockSize - 1000) + 3 * HeaderSize}
)

func (t *LogTest) init() {
  var err error
  if t.writer, err = NewWriter("/tmp/log_test"); err != nil {
    t.assert.Errorf("build log writer failed %s", err.Error())
  }

  if t.reader, err = NewReader("/tmp/log_test", true, 0); err != nil {
    t.assert.Errorf("build log reader failed %s", err.Error())
  }
  t.reading = false
  t.dest = new(StringDest)
  t.source = new (StringSource)
  t.writtenBytes = 0
}

func (t *LogTest) Write(msg string) {
  if err := t.writer.AddRecord([]byte(msg)); err != nil {
    t.assert.Errorf("log writer add record failed %s", err.Error())
  }
  t.writtenBytes += len(msg) + HeaderSize
}

func (t *LogTest) Read() string {
  if !t.reading {
    t.reading = true
  }
  if rslt, err := t.reader.Read(); err != nil {
    return err.Error()
  } else {
    return string(rslt)
  }
}

func (t *LogTest) WriteInitialOffsetLog() {
  for i := 0; i < 4; i++ {
    data := strings.Repeat("a", initialOffsetRecordSize[i])
    t.Write(data)
  }
}

func (t *LogTest) CheckOffsetPastEndReturnsNoRecords(offsetPastEnd uint32) {
  t.WriteInitialOffsetLog()
  t.reading = true
  var err error
  t.reader, err = NewReader("/tmp/log_test", true, uint32(t.writtenBytes) + offsetPastEnd)

  if err != nil {
    t.assert.Errorf("create past end log reader failed %s", err.Error())
  }

  if _, err = t.reader.Read(); err != nil {
    t.assert.Logf("check offset past end return past %s", err.Error())
  }
}

func (t *LogTest) CheckInitialOffsetRecord(initialOffset uint32, expected uint32) {
  t.WriteInitialOffsetLog()
  t.reader.Close()
  t.reader = nil
  var err error
  t.reader, err = NewReader("/tmp/log_test", true, initialOffset)
  msg, err := t.reader.Read()

  if err != nil {
    t.assert.Errorf("read log init offset failed %s", err.Error())
  }

  if initialOffsetRecordSize[expected] != len(msg) {
    t.assert.Errorf("read log size not as expected %d %d %d", initialOffset, initialOffsetRecordSize[expected], len(msg))
  }
}

func TestLog(t *testing.T) {
  test := new(LogTest)
  test.assert = t
  test.init()

  test.Write("foo")
  test.Write("bar")
  test.Write("")
  test.Write("xxxx")


  if v := test.Read(); v != "foo" {
    test.assert.Errorf("read match foo failed %s", v)
  }

  if v := test.Read(); v != "bar" {
    test.assert.Errorf("read match bar failed %s", v)
  }

  if v := test.Read(); v != "" {
    test.assert.Errorf("read match emtpy failed %s", v)
  }

  if v := test.Read(); v != "xxxx" {
    test.assert.Errorf("read match xxxx failed %s", v)
  }

  if v := test.Read(); v != "EOF" {
    test.assert.Errorf("read log not eof1")
  }

  if v := test.Read(); v != "EOF" {
    test.assert.Errorf("read log not eof2")
  }

  test.writer.Close()
  test.reader.Close()
  os.Remove("/tmp/log_test")

  test = new(LogTest)
  test.assert = t
  test.init()

  test.Write("small")
  test.Write(BigString("medium", 50000))
  test.Write(BigString("large", 100000))

  if v := test.Read(); v != "small" {
    test.assert.Errorf("read small match failed %s", v)
  }

  if v := test.Read(); v != BigString("medium", 50000) {
    test.assert.Errorf("read medium match failed %v", v)
  }

  if v := test.Read(); v != BigString("large", 100000) {
    test.assert.Errorf("read large match failed")
  }

  test.writer.Close()
  test.reader.Close()
  os.Remove("/tmp/log_test")

  test = new(LogTest)
  test.assert = t
  test.init()

  for i := 0; i < 100000; i++ {
    test.Write(NumberString(i))
  }

  for i := 0; i < 100000; i++ {
    if v := test.Read(); v != NumberString(i) {
      test.assert.Errorf("read numb str %d match failed %s", i, v)
    }
  }
  test.writer.Close()
  test.reader.Close()
  os.Remove("/tmp/log_test")
  
  marginal(t)
  wrapper(t, marginal2)
  wrapper(t, shortTrailer)
  wrapper(t, aliagn)
  wrapper(t, randRead) 
  wrapper(t, cof1) 
  wrapper(t, cof2)
  wrapper(t, cof3)
  wrapper(t, cof4)
  wrapper(t, cof5)
  wrapper(t, cof6)
  wrapper(t, cof7)
  wrapper(t, cof8)
  wrapper(t, cof9)
  wrapper(t, cof10)
  wrapper(t, cof11)
  wrapper(t, cpf1)
  wrapper(t, cpf2)
}

func randRead(t *LogTest) {
}

func cof1(t *LogTest) {
  t.CheckInitialOffsetRecord(0, 0)
}

func cof2(t *LogTest) {
  t.CheckInitialOffsetRecord(1, 1)
}

func cof3(t *LogTest) {
  t.CheckInitialOffsetRecord(10000, 1)
}

func cof4(t *LogTest) {
  t.CheckInitialOffsetRecord(10007, 1)
}

func cof5(t *LogTest) {
  t.CheckInitialOffsetRecord(10008, 2)
}

func cof6(t *LogTest) {
  t.CheckInitialOffsetRecord(20014, 2)
}

func cof7(t *LogTest) {
  t.CheckInitialOffsetRecord(20015, 3)
}

func cof8(t *LogTest) {
  t.CheckInitialOffsetRecord(BlockSize - 4, 3)
}

func cof9(t *LogTest) {
  t.CheckInitialOffsetRecord(BlockSize + 1, 3)
}

func cof10(t *LogTest) {
  t.CheckInitialOffsetRecord(2 * BlockSize + 1, 3)
}

func cof11(t *LogTest) {
  t.CheckInitialOffsetRecord(
    2 * (HeaderSize + 1000) + (2 * BlockSize - 1000) + 3 * HeaderSize,
    3)
}
 
func cpf1(t *LogTest) {
  t.CheckOffsetPastEndReturnsNoRecords(0)
}

func cpf2(t *LogTest) {
  t.CheckOffsetPastEndReturnsNoRecords(5)
}

func aliagn(t *LogTest) {
  n := BlockSize - 2 * HeaderSize + 4
  t.Write(BigString("foo", n))
  if t.writtenBytes != BlockSize - HeaderSize + 4 {
    t.assert.Errorf("short trailer write failed")
  }

  if v := t.Read(); v != BigString("foo", n) {
    t.assert.Errorf("read marginal foo match failed")
  }

  if v := t.Read(); v != "EOF" {
    t.assert.Errorf("read marginal EOF match failed") 
  }
}

func shortTrailer(t *LogTest) {
  n := BlockSize - 2 * HeaderSize + 4
  t.Write(BigString("foo", n))
  if t.writtenBytes != BlockSize - HeaderSize + 4 {
    t.assert.Errorf("short trailer write failed")
  }

  t.Write("")
  t.Write("bar")

  if v := t.Read(); v != BigString("foo", n) {
    t.assert.Errorf("read marginal foo match failed")
  }

  if v := t.Read(); v != "" {
    t.assert.Errorf("read marginal empty match failed") 
  }

  if v := t.Read(); v != "bar" {
    t.assert.Errorf("read marginal bar match failed") 
  }

  if v := t.Read(); v != "EOF" {
    t.assert.Errorf("read marginal EOF match failed") 
  }
}

func marginal(t *testing.T) {
  test := new(LogTest)
  test.assert = t
  test.init()

  n := BlockSize - 2 * HeaderSize
  test.Write(BigString("foo", n))
  if BlockSize - HeaderSize != test.writtenBytes {
    test.assert.Errorf("write marginal failed")
  }
  test.Write("")
  test.Write("bar")
  if v := test.Read(); v != BigString("foo", n) {
    test.assert.Errorf("read marginal foo match failed")
  }

  if v := test.Read(); v != "" {
    test.assert.Errorf("read marginal empty match failed") 
  }

  if v := test.Read(); v != "bar" {
    test.assert.Errorf("read marginal bar match failed") 
  }

  if v := test.Read(); v != "EOF" {
    test.assert.Errorf("read marginal EOF match failed") 
  }

  test.writer.Close()
  test.reader.Close()
  os.Remove("/tmp/log_test")
}

func marginal2(test *LogTest) {
  n := BlockSize - 2 * HeaderSize
  test.Write(BigString("foo", n))

  if BlockSize - HeaderSize != test.writtenBytes {
    test.assert.Errorf("write marginal2 failed")
  }

  test.Write("bar")

  if v := test.Read(); v != BigString("foo", n) {
    test.assert.Errorf("read marginal2 bar match failed") 
  }

  if v := test.Read(); v != "bar" {
    test.assert.Errorf("read marginal2 bar match failed") 
  }
}

func wrapper(t *testing.T, exmain func(*LogTest)) {
  test := new(LogTest)
  test.assert = t
  test.init()
  exmain(test)
  test.writer.Close()
  test.reader.Close()
  os.Remove("/tmp/log_test")
}
