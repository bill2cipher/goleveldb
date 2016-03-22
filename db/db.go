package db

import (
  "sync"
)

import (
  "github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/mem"
  "github.com/jellybean4/goleveldb/compact"
)

type DB interface {
  // Set the database entry for "key" to "value".  Returns OK on success,
  // and a non-OK status on error.
  // Note: consider setting options.sync = true.
  Put(option *util.WriteOption, key, value []byte) error
  
  // Remove the database entry (if any) for "key".  Returns OK on
  // success, and a non-OK status on error.  It is not an error if "key"
  // did not exist in the database.
  // Note: consider setting options.sync = true. 
  Delete(option *util.WriteOption, key []byte) error
  
  // Apply the specified updates to the database.
  // Returns OK on success, non-OK on failure.
  // Note: consider setting options.sync = true. 
  Write(option *util.WriteOption, batch *WriteBatch) error
  
  // If the database contains an entry for "key" store the
  // corresponding value in *value and return OK.
  //
  // If there is no entry for "key" leave *value unchanged and return
  // a status for which Status::IsNotFound() returns true.
  //
  // May return some other Status on an error.
  Get(option *util.ReadOption, key []byte) (error, []byte)
  
  // Return a heap-allocated iterator over the contents of the database.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  //
  // Caller should delete the iterator when it is no longer needed.
  // The returned iterator should be deleted before this db is deleted.
  NewIterator(option *util.ReadOption) mem.Iterator
  
  // Return a handle to the current DB state.  Iterators created with
  // this handle will all observe a stable snapshot of the current DB
  // state.  The caller must call ReleaseSnapshot(result) when the
  // snapshot is no longer needed.  
  GetSnapshot() Snapshot
  
  // DB implementations can export properties about their state
  // via this method.  If "property" is a valid property understood by this
  // DB implementation, fills "*value" with its current value and returns
  // true.  Otherwise returns false.
  //
  //
  // Valid property names include:
  //
  //  "leveldb.num-files-at-level<N>" - return the number of files at level <N>,
  //     where <N> is an ASCII representation of a level number (e.g. "0").
  //  "leveldb.stats" - returns a multi-line string that describes statistics
  //     about the internal operation of the DB.
  //  "leveldb.sstables" - returns a multi-line string that describes all
  //     of the sstables that make up the db contents.
  GetProperty(property []byte) (error, []byte)
  
  // For each i in [0,n-1], store in "sizes[i]", the approximate
  // file system space used by keys in "[range[i].start .. range[i].limit)".
  //
  // Note that the returned sizes measure file system space usage, so
  // if the user data compresses by a factor of ten, the returned
  // sizes will be one-tenth the size of the corresponding user data size.
  //
  // The results may not include the sizes of recently written data. 
  GetApproximateSizes(trange []Range) []uint64
  
  // Compact the underlying storage for the key range [*begin,*end].
  // In particular, deleted and overwritten versions are discarded,
  // and the data is rearranged to reduce the cost of operations
  // needed to access the data.  This operation should typically only
  // be invoked by users who understand the underlying implementation.
  //
  // begin==NULL is treated as a key before all keys in the database.
  // end==NULL is treated as a key after all keys in the database.
  // Therefore the following call will compact the entire database:
  //    db->CompactRange(NULL, NULL);
  CompactRange(begin, end []byte) error  
}

// Open the database with the specified "name".
// Stores a pointer to a heap-allocated database in *dbptr and returns
// OK on success.
// Stores NULL in *dbptr and returns a non-OK status on error.
// Caller should delete *dbptr when it is no longer needed.
func Open(option *util.Option, name string) DB {
  return nil
}


type dbImpl struct {
  mem     mem.Memtable
  imm     mem.Memtable
  compact compact.Compact
  batches []*writer
  mutex   *sync.Mutex
  signal  *sync.Cond
  option  *util.Option
  name    string
}

type writer struct {
  batch  WriteBatch
  option *util.WriteOption
  state  int
}

const (
  writer_DONE = iota
  writer_UNDONE
)

func (db *dbImpl) init() {
  db.batches = []*writer{}
  db.mutex  = new(sync.Mutex)
  db.signal = sync.NewCond(db.mutex)
}

func (db *dbImpl) Put(option *util.WriteOption, key, value []byte) error {
  batch := NewWriteBatch()
  batch.Put(key, value)
  return db.Write(option, batch)
}

func (db *dbImpl) Delete(option *util.WriteOption, key []byte) error {
  batch := NewWriteBatch()
  batch.Delete(key)
  return db.Write(option, batch)
}

func (db *dbImpl) Write(option *util.WriteOption, batch WriteBatch) error {
  w := &writer{batch, option, writer_UNDONE}
  db.mutex.Lock()
  db.batches = append(db.batches, w)
  
  for w.state == writer_UNDONE || w != db.batches[0] {
    db.signal.Wait()
  }
  db.mutex.Unlock()
  
  if w.state == writer_DONE {
    return nil
  }
  
  
  return nil
}

// Make room for key/value pairs if there's too many data in the mem
func (db *dbImpl) makeRoomForWrite(force bool) {
  for true {
    // there's still room in the mem
    if !force && db.mem.ApproximateMemoryUsage() < db.option.BufferSize {
      return
    }
    
    // there's no room in mem and imm is still under compaction
    if db.imm != nil {
      db.compact.Wait4Compact()
      continue
    }
    
    // there's too many level0 files
    if db.compact.NumLevelFiles(0) > util.Global.L0_StopWritesTrigger() {
      db.compact.Wait4Compact()
      continue
    }
    
    // Attempt to switch to a new memtable and trigger compaction of old memtable
    db.imm = db.mem
    db.mem = mem.NewMemtable(db.option.Comparator)
    filenum := db.version.NewFileNumber()
    filename := util.TableFileName(db.name, filenum)
    logger := log.NewWriter(filename)
    db.logger.Close()
    db.logger = logger
    ScheduleCompaction()
  }
}
