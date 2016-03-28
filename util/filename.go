package util

import (
  "os"
  "fmt"
  "strings"
)


const (
  Invalid = iota
  LogFile
  DBLockFile
  TableFile
  DescriptorFile
  CurrentFile
  TempFile
  InfoLogFile
)

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
func LogFileName(dbname string, number int) string {
  return fmt.Sprintf("%s/%06d.%s", dbname, number, "log")
}

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
func TableFileName(dbname string, number int) string {
  return fmt.Sprintf("%s/%06d.%s", dbname, number, "ldb")
}

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
func DescriptorFileName(dbname string, number int) string {
  return fmt.Sprintf("%s/MANIFEST-%06d", dbname, number)
}

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
func CurrentFileName(dbname string) string {
  return dbname + "/CURRENT"
}

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
func LockFileName(dbname string) string {
  return dbname + "/LOCK"
}

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
func TempFileName(dbname string, number int) string {
  return fmt.Sprintf("%s/%06d.%s", dbname, number, "dbtmp")
}

// Return the name of the info log file for "dbname".
func InfoLogFileName(dbname string) string {
  return dbname + "/LOG"
}

// Return the name of the old info log file for "dbname".
func OldInfoLogFileName(dbname string) string {
  return dbname + "/LOG.old"
}

// If filename is a leveldb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
func ParseFileName(filename string) (int, int) {
  shorts := strings.Split(filename, "\\")
  short  := shorts[len(shorts) - 1] 
  var num int
  var rtype string
  
  if strings.HasPrefix(short, "MANIFEST-") {
    fmt.Sscanf(short, "MANIFEST-%06d", &num)
    return num, DescriptorFile
  } else if strings.HasPrefix(short, "CURRENT") {
    return 0, CurrentFile
  } else if strings.HasPrefix(short, "LOCK") {
    return 0, DBLockFile
  } else if strings.HasPrefix(short, "LOG") {
    return 0, InfoLogFile
  }
  
  fmt.Sscanf(short, "%06d.%s", &num, &rtype)
  switch rtype {
  case "log":
    return num, LogFile
  case "ldb":
    return num, TableFile
  case "dbtmp":
    return num, TempFile
  }
  
  return -1, -1
}
// Make the CURRENT file point to the descriptor file with the
// specified number.
func SetCurrentFile(dbname string, num int) error {
  filename := CurrentFileName(dbname)
  if file, err := os.OpenFile(filename, os.O_TRUNC | os.O_WRONLY | os.O_CREATE, 0660); err != nil {
    return err
  } else {
    desc := DescriptorFileName(dbname, num)
    file.WriteString(desc + "\n")
    file.Close()
  }
  return nil
}
