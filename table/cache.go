package table

import (
  "container/ring"
)

import (
  "github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/mem"
)

type TableCache interface {

  // Find the specified table within cache
  FindTable(num int, fileSize int) Table
  
  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-NULL, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or NULL if no Table object underlies
  // the returned iterator.
  NewIterator(num int, fileSize int) (Table, mem.Iterator)

  // If a seek to internal key "k" in specified file finds an entry, return key and value
  Get(option *util.ReadOption, num, fileSize int, key []byte) ([]byte, []byte)

  // Evict any entry for the specified file number
  Evict(num int)
}

func NewTableCache(dbname string, option *util.Option, entries int) TableCache {
  cache := new(cacheImpl)
  if err := cache.init(dbname, option, entries); err != nil {
    return nil
  } 
  return cache
}

type cacheImpl struct {
  dbname  string
  option  *util.Option
  entries int              // cache size limit
  cache   map[int]Table
  head    *ring.Ring
  size    int              // num of data within the cache
}

func (c *cacheImpl) init(dbname string, option *util.Option, entries int) error {
  c.dbname = dbname
  c.option = option
  c.entries = entries
  c.head = ring.New(1)
  c.size = 0
  c.cache = make(map[int]Table)
  return nil
}

func (c *cacheImpl) FindTable(num, fileSize int) Table {
  if table, ok := c.cache[num]; ok {
    return table
  }
  
  tableName := util.TableFileName(c.dbname, uint64(num))
  table := OpenTable(tableName, fileSize, c.option)
  if table == nil {
    return nil
  }
  
  for c.size >= c.entries {
    val := c.head.Unlink(1).Value.(int)
    delete(c.cache, val)
    c.size--
  }
  
  elem := ring.New(1)
  elem.Value = num
  c.head.Prev().Link(elem)
  c.size++

  return table
}

func (c *cacheImpl) NewIterator(num, fileSize int) (Table, mem.Iterator) {
  if table := c.FindTable(num, fileSize); table == nil {
    return nil, nil
  } else if iter := table.NewIterator(); iter == nil {
    return nil, nil
  } else {
    return table, iter
  }
}

func (c *cacheImpl) Get(option *util.ReadOption, num, fileSize int, key []byte) ([]byte, []byte) {
  table, iter := c.NewIterator(num, fileSize)
  if table == nil || iter == nil {
    return nil, nil
  }
  iter.Seek(key)
  if iter.Valid() && util.BinaryCompare(iter.Key().([]byte), key) == 0 {
    return iter.Key().([]byte), iter.Value().([]byte)
  }
  return nil, nil
}

func (c *cacheImpl) Evict(num int) {
  current := c.head.Next()
  for current != c.head {
    n := current.Value.(int)
    if n == num {
        current.Prev().Unlink(1)
    }
    current = current.Next()
  }
  delete(c.cache, num)
  c.size--
}
