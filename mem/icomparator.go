package mem

import "encoding/binary"

import "github.com/jellybean4/goleveldb/util"

// InternalKeyComparator is used for internal key compare 
type InternalKeyComparator struct {
  cmp util.Comparator
}

// NewInternalKeyComparator returns a new InternalKeyComparator
func NewInternalKeyComparator(cmp util.Comparator) *InternalKeyComparator {
  icmp := new(InternalKeyComparator)
  icmp.cmp = cmp
  return icmp
}

// Compare returns compare rslt of a and b
func (i *InternalKeyComparator) Compare(a, b interface{}) int {
  val1 := a.([]byte)
  val2 := b.([]byte)
  split1 := len(val1) - 8
  split2 := len(val2) - 8
  cmp := i.cmp.Compare(val1[:split1], val2[:split2])
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

func (i *InternalKeyComparator) Name() string {
  return "internal_key_comparator"
}

// UserComparator returns internal user comparator
func (i *InternalKeyComparator) UserComparator() util.Comparator {
  return i.cmp
}

// FindShortestSep finds shortest key within ikey1 and ikey2
func (i *InternalKeyComparator) FindShortestSep(ikey1, ikey2 interface{}) interface{} {
  ukey1 := util.ExtractUserKey(ikey1.([]byte))
  ukey2 := util.ExtractUserKey(ikey2.([]byte))
  rslt := i.cmp.FindShortestSep(ukey1, ukey2).([]byte)
  if len(rslt) < len(ukey1) && i.cmp.Compare(ukey1, rslt) < 0 {
    store := make([]byte, 8)
    binary.LittleEndian.PutUint64(store,
        util.PackSeqAndType(util.Global.MaxSeq, SeekType))
    rslt = append(rslt, store...)
    return rslt
  }
  return ikey1
}

// MemtableKeyComparator is used to compare memtable entry key
type MemtableKeyComparator struct {
  icmp util.Comparator
}

// NewMemtableKeyComparator returns a new comparator
func NewMemtableKeyComparator(cmp util.Comparator) util.Comparator {
  comp := new(MemtableKeyComparator)
  comp.icmp = cmp
  return comp
}

// Compare returns compare rslt of a and b
func (m *MemtableKeyComparator) Compare(a, b interface{}) int {
  a1, b1 := a.([]byte), b.([]byte)
  val1, _ := util.GetLenPrefixBytes(a1)
  val2, _ := util.GetLenPrefixBytes(b1)
  return m.icmp.Compare(val1, val2)
} 

// FindShortestSep finds shortest key within ikey1 and ikey2
func (m *MemtableKeyComparator) FindShortestSep(ikey1, ikey2 interface{}) interface{} {
  return ikey1
}

func (m *MemtableKeyComparator) Name() string {
  return "memtable_key_comparator"
}
