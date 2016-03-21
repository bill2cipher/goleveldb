package filter

import (
  "fmt"
  "math/rand"
  "testing"
)

func TestFilter(t *testing.T) {
  filter := NewBloomPolicy(16)
  var keys [][]byte
  for i := 0; i < 10000; i++ {
    key := fmt.Sprintf("key%d", i)
    keys = append(keys, []byte(key))
  }
  data := filter.CreateFilter(keys)


  for i := 0; i < 10000; i++ {
    key := fmt.Sprintf("key%d", i)
    if filter.KeyMayMatch([]byte(key), data) == false {
      t.Errorf("exist key %s not match", key)
    }
  }
}

func TestEmpty(t *testing.T) {
  policy := NewBloomPolicy(16)
  builder := NewBlockBuilder(policy)

  rslt := builder.Finish()
  tmp := []byte{0x0, 0x0, 0x0, 0x0, 0x0b}
  if len(rslt) != 5 {
    t.Errorf("empty rslt len %d wrong", len(rslt))
  }

  for i := 0; i < 5; i++ {
    if tmp[i] != rslt[i] {
      t.Errorf("emtpy rslt wrong %v", rslt)
    }
  }

  reader := NewBlockReader(policy, rslt)
  if reader == nil {
    t.Errorf("build reader failed")
  }
  if reader.KeyMayMatch(0, []byte("foo")) == false {
    t.Errorf("empty match wrong1")
  }

  if reader.KeyMayMatch(1000000, []byte("foo")) == false {
    t.Errorf("empty match wrong2")
  }
}

func TestSingle(t *testing.T) {
  policy := NewBloomPolicy(16)
  builder := NewBlockBuilder(policy)
  builder.StartBlock(100)

  builder.AddKey([]byte("foo"))
  builder.AddKey([]byte("bar"))
  builder.AddKey([]byte("box"))
  builder.StartBlock(200)

  builder.AddKey([]byte("box"))
  builder.StartBlock(300)

  builder.AddKey([]byte("hello"))
  
  block := builder.Finish()
  reader := NewBlockReader(policy, block)
  if reader == nil {
    t.Errorf("build test single reader failed")
  }

  if reader.KeyMayMatch(100, []byte("foo")) == false {
    t.Errorf("key not match foo")
  }

  if reader.KeyMayMatch(100, []byte("bar")) == false {
    t.Errorf("key not match bar")
  }  

  if reader.KeyMayMatch(100, []byte("box")) == false {
    t.Errorf("key not match box")
  }

  if reader.KeyMayMatch(100, []byte("hello")) == false {
    t.Errorf("key not match hello")
  }

  if reader.KeyMayMatch(100, []byte("foo")) == false {
    t.Errorf("key not match foo")
  }

  if reader.KeyMayMatch(100, []byte("jacky")) == true {
    t.Errorf("key match miss")
  }

  if reader.KeyMayMatch(100, []byte("other")) == true {
    t.Errorf("key match other")
  }
}

func TestMulti(t *testing.T) {
  policy := NewBloomPolicy(16)
  builder := NewBlockBuilder(policy)
  builder.StartBlock(0)
  builder.AddKey([]byte("foo"))
  builder.StartBlock(2000)
  builder.AddKey([]byte("bar"))

  builder.StartBlock(3100)
  builder.AddKey([]byte("box"))

  builder.StartBlock(9000)
  builder.AddKey([]byte("box"))
  builder.AddKey([]byte("hello"))

  block := builder.Finish()
  reader := NewBlockReader(policy, block)

  if reader.KeyMayMatch(0, []byte("foo")) == false {
    t.Errorf("key not match foo")
  }

  if reader.KeyMayMatch(2000, []byte("bar")) == false {
    t.Errorf("key not match bar")
  }

  if reader.KeyMayMatch(0, []byte("box")) == true {
    t.Errorf("box match bar")
  }

  if reader.KeyMayMatch(0, []byte("hello")) == true {
    t.Errorf("hello match bar")
  }

  if reader.KeyMayMatch(3100, []byte("box")) == false {
    t.Errorf("box not match")
  }
  if reader.KeyMayMatch(3100, []byte("foo")) == true {
    t.Errorf("foo match box")
  }
  if reader.KeyMayMatch(3100, []byte("bar")) == true {
    t.Errorf("bar match box")
  }
  if reader.KeyMayMatch(3100, []byte("bill")) == true {
    t.Errorf("bill match box")
  }

  if reader.KeyMayMatch(4100, []byte("foo")) == true {
    t.Errorf("foo match empty")
  }
  if reader.KeyMayMatch(4100, []byte("bar")) == true {
    t.Errorf("bar match empty")
  }
  if reader.KeyMayMatch(4100, []byte("box")) == true {
    t.Errorf("box match empty")
  }

  if reader.KeyMayMatch(9000, []byte("box")) == false {
    t.Errorf("box not match")
  }
  if reader.KeyMayMatch(9000, []byte("hello")) == false {
    t.Errorf("box not match")
  }
    if reader.KeyMayMatch(9000, []byte("foo")) == true {
    t.Errorf("foo match empty")
  }
  if reader.KeyMayMatch(9000, []byte("bar")) == true {
    t.Errorf("bar match empty")
  }
}


func TestBuilder(t *testing.T) {
  policy := NewBloomPolicy(16)
  builder := NewBlockBuilder(policy)
  offset := 0
  list := []int{0}

  for i := 0; i < 10000; i++ {
    key := fmt.Sprintf("key%d", i)
    builder.AddKey([]byte(key))

    if i % 100 == 99 {
      offset += 4096
      offset += rand.Int() % 4096
      list = append(list, offset)
      builder.StartBlock(offset)
    } 
  }

  rslt := builder.Finish()

  cnt := 0
  reader := NewBlockReader(policy, rslt)
  if reader == nil {
    t.Errorf("build reader failed")
  }
  for i := 0; i < 10000; i++ {
    key := fmt.Sprintf("key%d", i)
    offset := uint32(list[cnt])
    if reader.KeyMayMatch(offset, []byte(key)) == false {
      t.Errorf("exist key not match %v", key)
    }
    if i % 100 == 99 {
      cnt++
    }
  }
}