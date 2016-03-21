package util

import "encoding/binary"

type Comparator interface {
  Compare(a, b interface{}) int
}

func BinaryCompare(first, second []byte) int {
  var clen int
  if len(first) > len(second) {
    clen = len(second)
  } else {
    clen = len(first)
  }

  for i := 0; i < clen; i++ {
    switch true {
    case first[i] > second[i]:
      return 1;
    case first[i] < second[i]:
      return -1;
    }
  }

  if len(first) > len(second) {
    return 1
  } else if len(first) == len(second) {
    return 0
  } else {
    return -1
  }
}


type binaryCmp struct {
}

var BinaryComparator binaryCmp


func (binary binaryCmp) Compare(a, b interface{}) int {
  return BinaryCompare(a.([]byte), b.([]byte))
}


type InternalKeyComparator struct {

}

func NewInternalKeyComparator() Comparator {
  return new(InternalKeyComparator)
}

func (i *InternalKeyComparator) Compare(a, b interface{}) int {
  val1 := a.([]byte)
  val2 := b.([]byte)
  split1 := len(val1) - 8
  split2 := len(val2) - 8
  cmp := BinaryCompare(val1[:split1], val2[:split2])
  if cmp != 0 {
    return cmp
  }

  seq1 := binary.LittleEndian.Uint64(val1[split1:])
  seq2 := binary.LittleEndian.Uint64(val2[split2:])

  switch true {
  case seq2 > seq1 :
    return 1
  case seq1 == seq2:
    return 0
  default:
    return -1
  }
}