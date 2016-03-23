package mem

import "github.com/jellybean4/goleveldb/util"

type Iterator interface {

  // Returns true iff the iterator is positioned at a valid state
  Valid() bool

  // Returns the key the iterator is currently positioned at
  Key() interface{}

  // Returns the value the iterator is currently positioned at
  Value() interface{}

  // Move to the next key
  Next()

  // Advances to the previous position
  Prev()

  // Advances to the position with key equal to the given key
  Seek(key interface{})

  // Advances to the first key within the list
  SeekToFirst()

  // Advances to the last key within the list
  SeekToLast()
}


// Skiplist is a type of ordered list, it's quick to 
// search a key within the list
type Skiplist interface {

  // Insert a key into the list, iff there's a key compares equal
  // to the inserted key within the list, return false; otherwise,
  // return true.
  Insert(key interface{}) bool

  // Returns true iff an entry that compares equal to key is in the
  // list
  Contains(key interface{}) bool

  // Create a new iterator for this list
  NewIterator() Iterator

  // Dump all data within this list
  DumpData() []interface{}
}


type MemEntry struct {
  Key []byte
  Val []byte
  Seq uint64
  Rtype byte
}

type Memtable interface {

  ApproximateMemoryUsage() int

  NewIterator() Iterator

  Add(seq uint64, rtype byte, key, value []byte)

  Get(key util.LookupKey) []byte

  DumpData() []MemEntry
}

const (
  SeekType = iota
  ValueType 
  DeleteType
  MaxType
)
