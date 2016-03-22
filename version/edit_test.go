package version

import (
  "fmt"
  "testing"
)

import (
  "github.com/jellybean4/goleveldb/util"
)

func TestSimpleEdit(t *testing.T) {
  cmpName := "BinaryComparator"
  logNumber := 10
  nextFile := 234
  var lastSeq uint64 = 23894230948
  
  edit := NewVersionEdit()
  edit.SetComparatorName(cmpName)
  edit.SetLogNumber(logNumber)
  edit.SetNextFile(nextFile)
  edit.SetLastSequence(lastSeq)
  rslt := edit.Encode()
  if rslt == nil {
    t.Errorf("encode data null")
  }
  
  edit2 := NewVersionEdit()
  if err := edit2.Decode(rslt); err != nil {
    t.Errorf("edit decode fail %v", err)
  }
  
  if edit2.GetComparatorName() != cmpName {
    t.Errorf("cmp name not match %v", edit2.GetComparatorName())
  }
  
  if edit2.GetLogNumber() != logNumber {
    t.Errorf("log num not match %d", edit2.GetLogNumber())
  }
  
  if edit2.GetNextFile() != nextFile {
    t.Errorf("file num not match %d", edit2.GetNextFile())
  }
  
  if edit2.GetLastSequence() != lastSeq {
    t.Errorf("last seq not match %d", edit2.GetLastSequence())
  }
}

func TestFullEdit(t *testing.T) {
  cmpName := "BinaryComparator"
  logNumber := 12312312
  nextFile := 1231231223
  var lastSeq uint64= 2349098567
  
  edit := NewVersionEdit()
  edit.SetComparatorName(cmpName)
  edit.SetLogNumber(logNumber)
  edit.SetNextFile(nextFile)
  edit.SetLastSequence(lastSeq)
  
  files := []FileMetaData{}
  for i := 0; i < 100; i++ {
    key := fmt.Sprintf("%s%d", "key", i)
    key2 := fmt.Sprintf("%s%d", "val", i)
    i2 := uint64(i)
    meta := FileMetaData {
      AllowSeek : 0,
      Number    : i,
      FileSize  : i * 100,
      Smallest  : *util.NewInternalKey([]byte(key), i2, byte(i % 2)),
      Largest   : *util.NewInternalKey([]byte(key2), i2, byte(i % 2)),
    }
    files = append(files, meta)
  }
  
  deletes := []int{}
  for i := 0; i < 100; i++ {
    deletes = append(deletes, i)
  }
  
  pointers := []*util.InternalKey{}
  for i := 0; i < 100; i++ {
    key := fmt.Sprintf("%s%d", "key", i)
    ikey := util.NewInternalKey([]byte(key), uint64(i), byte(i % 2))
    pointers = append(pointers, ikey)
  }

  for i := 0; i < 100; i++ {
    edit.AddFile(i, files[i].Number, files[i].FileSize, &files[i].Smallest, &files[i].Largest)
    edit.DeleteFile(i, deletes[i])
    edit.SetCompactPointer(i, pointers[i])
  }
  rslt := edit.Encode()
  
  
  edit2 := NewVersionEdit()
  if err := edit2.Decode(rslt); err != nil {
    t.Errorf("edit decode fail %v", err)
  }
  
  if edit2.GetComparatorName() != cmpName {
    t.Errorf("cmp name not match %v", edit2.GetComparatorName())
  }
  
  if edit2.GetLogNumber() != logNumber {
    t.Errorf("log num not match %d", edit2.GetLogNumber())
  }
  
  if edit2.GetNextFile() != nextFile {
    t.Errorf("file num not match %d", edit2.GetNextFile())
  }
  
  if edit2.GetLastSequence() != lastSeq {
    t.Errorf("last seq not match %d", edit2.GetLastSequence())
  }
  
  for i := 0; i < 100; i++ {
    file := edit2.GetFile(i)
    delete := edit2.GetDeletedFile(i)
    pointer := edit2.GetCompactPointer(i)
    
    if len(file) != 1 {
      t.Errorf("files len not one")
    }
    
    if len(delete) != 1 {
      t.Errorf("dele not one")
    }
    
    if len(pointer) != 1 {
      t.Errorf("pointer len not one")
    }
    
    if !ikCmp(pointer[0], pointers[i]) {
      t.Errorf("pointer content not match")
    }
    
    if !metaCmp(file[0], &files[i]) {
      t.Errorf("file content not match")
    }
    
    if delete[0] != deletes[i] {
      t.Errorf("delete not match %d %d", delete[0], deletes[i])
    }
  }
}

func ikCmp(i1, i2 *util.InternalKey) bool {
  b1 := i1.Encode()
  b2 := i2.Encode()
  if len(b1) != len(b2) {
    return false
  }
  
  for i := 0; i < len(b1); i++ {
    if b1[i] != b2[i] {
      return false
    }
  }
  return true
}

func metaCmp(f1, f2 *FileMetaData) bool {
  if f1.FileSize != f2.FileSize {
    return false
  }
  
  if f1.Number != f2.Number {
    return false
  }
  
  if !ikCmp(&f1.Smallest, &f2.Smallest) {
    return false
  }
  return true
}
