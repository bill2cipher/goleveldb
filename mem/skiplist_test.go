package mem

import (
  "sort"
  "hash/crc64"
  "sync/atomic"
  "sync"
  "testing"
  "encoding/binary"
  "math/rand"
)

type Key uint64

func (k Key) Compare(a, b interface{}) int {
  a1, b1 := a.(Key), b.(Key)
  if a1 < b1 {
    return -1
  } else if a1 == b1 {
    return 0
  } else {
    return 1
  }
}

func TestIter(t *testing.T) {
  list := NewSkiplist(Key(0))
  var d Key = 10
  if list.Contains(d) {
    t.Errorf("empty list contains 10")
  }

  iter := list.NewIterator()
  if iter.Valid() {
    t.Errorf("invalid iter reports valid")
  }

  iter.SeekToFirst()
  if iter.Valid() {
    t.Errorf("empty iter reports valid")
  }

  iter.Seek(Key(100))
  if iter.Valid() {
    t.Errorf("100 seek iter reports valid")
  } 

  iter.SeekToLast()
  if iter.Valid() {
    t.Errorf("last seek iter reports valid")
  }
}

func TestList(t *testing.T) {
  N, R := 2000, 5000
  list := NewSkiplist(Key(0))
  data := make(map[Key]interface{})
  for i := 0; i < N; i++ {
    var k Key = Key(rand.Int() % R)
    data[k] = nil
    list.Insert(k)
  }

  priorities := make([]int, 0)
  for k := range data {
    priorities = append(priorities, int(k))
  }
  sort.Ints(priorities)

  for i := 0; i < R; i++ {
    k := Key(i)
    _, ok := data[k]
    if list.Contains(k) && !ok {
      t.Errorf("list contains and map not %d", k)
    } else if !list.Contains(k) && ok {
      t.Errorf("list not contains but map does %d", k)
    }
  }

  iter := list.NewIterator()
  if iter.Valid() {
    t.Errorf("new iter valid")
  }

  iter.Seek(Key(priorities[0]))
  if !iter.Valid() {
    t.Errorf("seek first key invalid")
  } else if iter.Key().(Key) != Key(priorities[0]) {
    t.Errorf("seeked first key not equal")
  }

  iter.SeekToFirst()
  if !iter.Valid() {
    t.Errorf("seek to first invalid")
  } else if iter.Key().(Key) != Key(priorities[0]) {
    t.Errorf("seeked to first not equal")
  }

  iter.SeekToLast()
  if !iter.Valid() {
    t.Errorf("seek to last invalid")
  } else if iter.Key().(Key) != Key(priorities[len(priorities) - 1]) {
    t.Errorf("seeked to last not equal")
  }

  for i := 0; i < 1; i++ {
    iter = list.NewIterator()
    iter.Seek(Key(i))

    pos := 0
    for pos = 0; pos < len(priorities) && priorities[pos] < i; pos++ {
    }

    for j := 0; j < 3; j++ {
      if (pos >= len(priorities)) {
        if iter.Valid() {
          t.Errorf("iter valid at the end")
          break
        }
      } else {
        if !iter.Valid() {
          t.Errorf("iter seek not valid")
        }

        if k := iter.Key(); k == nil {
          t.Errorf("iter seek not find")
        } else if k.(Key) != Key(priorities[j]) {
          t.Errorf("iter seek not equal")
        }
        pos++
        iter.Next()
      }
    }
  }

  iter = list.NewIterator()
  iter.SeekToLast()

  for i := len(priorities) - 1; i >= 0; i-- {
    if !iter.Valid() {
      t.Errorf("backward seek invalid")
    } else if Key(priorities[i]) != iter.Key().(Key) {
      t.Errorf("backward seek not match %d", priorities[i])
    }
    iter.Prev()
  }

  if iter.Valid() {
    t.Errorf("backward seek end valid")
  }
}

type State []atomic.Value

func NewState() State {
  rslt := make([]atomic.Value, 4)
  for i := 0; i < 4; i++ {
    rslt[i].Store(uint64(0))
  }
  return State(rslt)
}

func (s State) set(k uint32, v uint64) {
  o := []atomic.Value(s)
  o[k].Store(v)
}

func (s State) get(k uint32) uint64 {
  o := []atomic.Value(s)
  return o[k].Load().(uint64)
}

type Concurrent struct {
  k uint32
  table *crc64.Table
  l Skiplist
  current State
  t *testing.T
}

func (c *Concurrent) init(t *testing.T) {
  c.table = crc64.MakeTable(crc64.ECMA)
  c.k = 4
  c.current = NewState()
  c.l = NewSkiplist(Key(0))
  c.t = t
}

func (c *Concurrent) key(k Key) uint64 {
  return uint64(k) >> 40
}

func (c *Concurrent) gen(k Key) uint64 {
  return uint64(k) >> 8 & 0xFFFFFFFF
}

func (c *Concurrent) hash(k Key) uint64 {
  return uint64(k & 0xFF)
}

func (c *Concurrent) hashNumber(k, g uint64) uint64 {
  buffer := make([]byte, 16)
  binary.LittleEndian.PutUint64(buffer, k)
  binary.LittleEndian.PutUint64(buffer[8:], g)
  h := crc64.New(c.table)
  h.Write(buffer)
  return h.Sum64()
}

func (c *Concurrent) makeKey(k, g uint64) Key {
  return Key((k << 40 | g << 8) | (c.hashNumber(k, g) & 0xFF))
}

func (c *Concurrent) isValidKey(k Key) bool {
  return c.hash(k) == (c.hashNumber(c.key(k), c.gen(k)) & 0xFF)
}


func (c *Concurrent) randomTarget() Key {
  switch rand.Int() % 10 {
  case 0 :
    return c.makeKey(0, 0);
  case 1:
    return c.makeKey(4, 0);
  default:
    return c.makeKey(uint64(rand.Int() % 4), 0)
  }
}

func (c *Concurrent) writeStep() {
  var k uint32 = rand.Uint32() % 4
  var g uint64 = c.current.get(k) + 1
  var key Key  = c.makeKey(uint64(k), g)
  c.l.Insert(key)
  c.current.set(k, g)
}

func (c *Concurrent) readStep() {
  initial := NewState()
  for k := uint32(0); k < 4; k++ {
    initial.set(k, c.current.get(k))
  }

  pos := c.randomTarget()
  iter := c.l.NewIterator()
  iter.Seek(pos)

  for true {
    var current Key
    if !iter.Valid() {
      current = c.makeKey(4, 0)
    } else {
      current = iter.Key().(Key)
      if !c.isValidKey(current) {
        h := c.hashNumber(c.key(current), c.gen(current)) & 0xFF
        c.t.Errorf("fetch key is not valid %d %d %d", current, c.hash(current), h)
      }
    }

    if pos > current {
      c.t.Errorf("should not go backwards %d %d", pos, current)
    }

    for pos < current {
      if c.key(pos) >= 4 {
        c.t.Errorf("pos should little than current")
      }

      if !(c.gen(pos) == 0 || c.gen(pos) > initial.get(uint32(c.key(pos)))) {
        c.t.Errorf("there should no data between pos and current")
      }

      if c.key(pos) < c.key(current) {
        pos = c.makeKey(c.key(pos) + 1, 0)
      } else {
        pos = c.makeKey(c.key(pos), c.gen(pos) + 1)
      }
    }

    if !iter.Valid() {
      break
    }

    if rand.Int() % 2 == 1 {
      iter.Next()
      pos = c.makeKey(c.key(pos), c.gen(pos) + 1)
    } else {
      new_target := c.randomTarget()
      if new_target > pos {
        pos = new_target
        iter.Seek(new_target)
      }
    }
  }
}

func TestCon(t *testing.T) {
  con := new(Concurrent)
  con.init(t)

  for i := 0; i < 10000; i++ {
    con.readStep()
    con.writeStep()
  }
}


const (
  STARTING = iota
  RUNNING
  DONE
)

type TestState struct {
  c *Concurrent
  m sync.Mutex
  s int
  co *sync.Cond
  b atomic.Value
  r int
}

func (s *TestState) init(t *testing.T) {
  s.c = new(Concurrent)
  s.c.init(t)
  s.co = sync.NewCond(&s.m)
  s.r = 0
}

func (t *TestState) Wait(s int) {
  t.m.Lock()
  for t.s != s {
    t.co.Wait()
  }
  t.m.Unlock()
}

func (t *TestState) Change(s int) {
  t.m.Lock()
  t.s = s
  t.co.Signal()
  t.m.Unlock()
}

func (t *TestState) SetQuit() {
  t.b.Store(true)
}

func (t *TestState) TestQuit() bool {
  return t.b.Load() != nil
}

func ConCurrentReader(s *TestState) {
  s.r++
  s.Change(RUNNING)
  reads := 0
  for !s.TestQuit() {
    s.c.readStep()
    reads++
  }
  s.Change(DONE)
}

func TestRun(t *testing.T) {
  for i := 0; i < 1000; i++ {
    s := new(TestState)
    s.init(t)
    go ConCurrentReader(s)
    s.Wait(RUNNING)
    for j := 0; j < 1000; j++ {
      s.c.writeStep()
    }
    s.SetQuit()
    s.Wait(DONE)
  }
}
