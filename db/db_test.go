package db

import (
  "fmt"
  "testing"
)

import (
  "github.com/jellybean4/goleveldb/util"
)

func TestSimpleDB(t *testing.T) {
  db := Open(&util.DefaultOption, "/tmp/test")
  cnt := 2000000
  for i := 0; i < cnt; i++ {
    key := fmt.Sprintf("key%d", i)
    val := fmt.Sprintf("val%d", i)
    if err := db.Put(&util.DefaultWriteOption, []byte(key), []byte(val)); err != nil {
      t.Errorf("put k/v error %v", err)
    }
  }
}

