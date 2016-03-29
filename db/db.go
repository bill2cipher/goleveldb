package db

import (
  "os"
  "sync"
  "sync/atomic"
  "errors"
)

import (
   "code.google.com/p/log4go"
)

import (
  "github.com/jellybean4/goleveldb/util"
  "github.com/jellybean4/goleveldb/mem"
  "github.com/jellybean4/goleveldb/table"
  "github.com/jellybean4/goleveldb/log"
  "github.com/jellybean4/goleveldb/version"
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
  Write(option *util.WriteOption, batch WriteBatch) error
  
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
  db := new(dbImpl)
  db.init(option, name)
  return db
}

func init() {
  config := os.Getenv("GOPATH") + "/res/log_config.xml"
  log4go.LoadConfiguration(config)
}

type dbImpl struct {
  mem     mem.Memtable
  imm     mem.Memtable
  batches []*writer
  mutex   *sync.Mutex
  bg_cv   *sync.Cond
  is_cmp  bool
  option  *util.Option
  name    string
  wlog    log.Writer
  vset    *version.VersionSet
  status  int
  shut    *atomic.Value
  cache   table.TableCache
}

type writer struct {
  batch  WriteBatch
  cv     *sync.Cond
  option *util.WriteOption
  state  int
  err    error
}

const (
  writer_DONE = iota
  writer_UNDONE
)

func (db *dbImpl) init(option *util.Option, name string) {
  icmp := mem.NewInternalKeyComparator(option.Comparator)
  option.Comparator = icmp

  db.batches = []*writer{}
  db.mutex  = new(sync.Mutex)
  db.bg_cv = sync.NewCond(db.mutex)
  db.status = 0
  db.is_cmp = false
  db.shut = new(atomic.Value)
  db.shut.Store(false)
  db.option = option
  db.name = name
  db.mem = mem.NewMemtable(icmp)
  db.cache = table.NewTableCache(name, option, util.Global.TableCacheEntries)
  db.vset = version.NewVersionSet(name, option, db.cache)
  
  if _, err := os.Stat(db.name); err != nil && os.IsNotExist(err) {
    os.Mkdir(db.name, os.ModePerm)
    log4go.Info("Making directory %s for new db", db.name)
  } else if err != nil {
    log4go.Info("Stating db directory error %v", err)
    panic(err.Error())
  }
  
  filenum := db.vset.NewFileNumber()
  filename := util.LogFileName(name, filenum)
  if wlog, err := log.NewWriter(filename); err != nil {
    log4go.Error("init log writer failed %s", err.Error())
    panic(err.Error())
  } else {
    db.wlog = wlog
  }
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
  w := &writer{batch, sync.NewCond(db.mutex), option, writer_UNDONE, nil}
  db.mutex.Lock()
  defer db.mutex.Unlock()

  db.batches = append(db.batches, w)
  for w.state == writer_UNDONE && w != db.batches[0] {
    w.cv.Wait()
  }

  if w.state == writer_DONE {
    return w.err
  }
  
  group := NewWriteBatch()
  seq := db.vset.LastSequence()
  handler := NewBatchHandler(db.mem, db.vset.LastSequence())

  err := db.makeRoomForWrite(batch == nil)
  var last *writer = w
  if err != nil {
    log4go.Error("make room for write failed %v", err)
    goto finish
  }
  
  for _, later := range db.batches {
    group.Append(later.batch)
    last = later
  }
  db.mutex.Unlock()
  
  err = db.wlog.AddRecord(group.Contents())
  if err != nil {
    log4go.Error("add log record failed %v", err)
    goto finish
  }

  batch.SetSequence(seq)
  err = batch.Iterate(handler)
  db.mutex.Lock()
  db.vset.SetLastSequence(seq + uint64(batch.Count()))
  if err != nil {
    log4go.Error("add k/v pairs into mem failed %v", err)
  }

finish:
  var idx int
  var later *writer
  for idx, later = range db.batches {
    if later == last {
      break
    }
    later.state = writer_DONE
    later.err = err
    later.cv.Signal()
  }
  
  db.batches = db.batches[idx + 1:]
  return w.err
}

func (db *dbImpl) CompactRange(begin, end []byte) error {
  return nil
}

func (db *dbImpl) Get(option *util.ReadOption, key []byte) (error, []byte) {
  return nil, nil
}

func (db *dbImpl) NewIterator(option *util.ReadOption) mem.Iterator {
  return nil
}

func (db *dbImpl) GetSnapshot() Snapshot {
  return nil
}

func (db *dbImpl) GetProperty(property []byte) (error, []byte) {
  return nil, nil
}

func (db *dbImpl) GetApproximateSizes(trange []Range) []uint64 {
  return nil
}

// Make room for key/value pairs if there's too many data in the mem
func (db *dbImpl) makeRoomForWrite(force bool) error {
  for true {
    if db.status == 1 {
      return errors.New("db is in wrong status")
    }
    // there's still room in the mem
    if !force && db.mem.ApproximateMemoryUsage() < db.option.BufferSize {
      return nil
    }
    
    // there's no room in mem and imm is still under compaction
    if db.imm != nil {
      db.bg_cv.Wait()
      continue
    }
    
    // there's too many level0 files
    if db.vset.NumLevelFiles(0) > util.Global.L0StopWritesTrigger {
      db.bg_cv.Wait()
      continue
    }
    
    // Attempt to switch to a new memtable and trigger compaction of old memtable
    lognum := db.vset.NewFileNumber()
    logname := util.LogFileName(db.name, lognum)
    if logger, err := log.NewWriter(logname); err != nil {
      db.vset.ReuseFileNumber(lognum)
      return err
    } else {
      db.imm = db.mem
      db.mem = mem.NewMemtable(db.option.Comparator)
      db.wlog.Close()
      db.wlog = logger
      db.vset.SetLogNumber(lognum)
      db.mayScheduleCompaction()
    }
    return nil
  }
  return nil
}

func (db *dbImpl) mayScheduleCompaction() {
  // under compaction already or db is about to shut down
  if db.is_cmp || db.shut.Load().(bool) == true{
    return
  }
  
  // db under wrong status
  if db.status != 0 {
    return
  }
  
  // db need no more compaction
  if db.imm == nil && !db.vset.NeedsCompaction() {
    return
  }
  go db.doCompaction()
}

func (db *dbImpl) doCompaction() {
  db.mutex.Lock()
  defer func() {
    db.is_cmp = false
    db.mutex.Unlock()
    db.bg_cv.Broadcast()
    db.mayScheduleCompaction()
  }()

  if db.imm != nil {
    db.is_cmp = true
    db.compactMemtable()
    return
  }
  
  if db.vset.NeedsCompaction() {
    db.is_cmp = true
    db.compactTableFiles()
    return
  }
}

func (db *dbImpl) compactMemtable() {
  memtable := db.imm

  iter := memtable.NewIterator()
  if iter.SeekToFirst(); !iter.Valid() {
    return
  }
  smallest := util.ExtractUserKey(iter.Key().([]byte))
  
  if iter.SeekToLast(); !iter.Valid() {
    return
  }
  largest := util.ExtractUserKey(iter.Key().([]byte))

  level := db.vset.Current().PickLevelForMemTableOutput(smallest, largest)
  filenum := db.vset.NewFileNumber()
  
  edit := version.NewVersionEdit()
  edit.SetLogNumber(db.vset.LogNumber())
  
  db.mutex.Unlock()
  meta := db.writeLevel0File(level, filenum, memtable)
  db.mutex.Lock()
  
  edit.AddFile(level, meta.Number, meta.FileSize, &meta.Smallest, &meta.Largest)
  db.vset.LogAndApply(edit)
  db.imm = nil
}

func (db *dbImpl) writeLevel0File(level, filenum int, imm mem.Memtable) *table.FileMetaData {
  filename := util.TableFileName(db.name, filenum)
  builder  := table.NewTableBuilder(filename, db.option)
  iter := imm.NewIterator()
  
  iter.SeekToFirst() 
  var large, small []byte
  small = iter.Key().([]byte)
  for iter.Valid() {
    builder.Add(iter.Key().([]byte), iter.Value().([]byte))
    large = iter.Key().([]byte)
    iter.Next()
  }
  builder.Finish()
  
  ismall := new(util.InternalKey)
  ismall.Decode(small)
  
  ilarge := new(util.InternalKey)
  ilarge.Decode(large)
  
  meta := new(table.FileMetaData)
  meta.FileSize = builder.FileSize()
  meta.Number = filenum
  meta.Largest = *ilarge
  meta.Smallest = *ismall
  meta.AllowSeek = 1
  return meta 
}

func (db *dbImpl) compactTableFiles() error {
  comp := db.vset.PickCompaction()
  if comp == nil {
    return errors.New("pick new compaction failed")
  }
  
  db.mutex.Unlock()
  iter := db.vset.MakeInputIterator(comp)
  tableNum := db.vset.NewFileNumber()
  tableName := util.TableFileName(db.name, tableNum)
  builder := table.NewTableBuilder(tableName, db.option)
  iter.SeekToFirst()
  log4go.Info("%s into %d with iter state %v", comp.Dump(), tableNum, iter.Valid())
  for iter.Valid() {
    builder.Add(iter.Key().([]byte), iter.Value().([]byte))
    iter.Next()
  }
  
  edit := version.NewVersionEdit()
  for i := 0; i < len(comp.Files); i++ {
    for _, meta := range comp.Files[i] {
      edit.DeleteFile(comp.Level + i, meta.Number)
    } 
  }
  
  db.mutex.Lock()
  edit.AddFile(comp.Level + 1, tableNum, builder.FileSize(), comp.Smallest, comp.Largest)
  db.vset.LogAndApply(edit)
  return nil
}
