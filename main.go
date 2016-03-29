package main
import (
  "fmt"
  "github.com/jellybean4/goleveldb/util"
)

func main() {
  s1 := []byte("key1000000")
  l1 := []byte("key999999")
  
  //s2 := []byte("key1110314")
  //l2 := []byte("key1226823")
  
  fmt.Printf("%d\n", util.BinaryCompare(s1, l1))
  fmt.Printf("%d\n", util.BinaryCompare(l1, s1))
}
