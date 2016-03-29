package version

import (
  "sort"
)

import (
	"github.com/jellybean4/goleveldb/table"
  "github.com/jellybean4/goleveldb/util"
)

// Builder is a helper class 
type Builder interface {
  // Apply edit to the current builder
  Apply(edit *VersionEdit)
  
  // Finish apply all edit to the version
  Finish(v *Version)
}

type builderImpl struct {
  files []map[int]*table.FileMetaData
  cmp   util.Comparator
}

func NewVersionBuilder(base *Version, cmp util.Comparator) Builder {
  builder := new(builderImpl)
  builder.init(base, cmp)
  return builder
}

func (b *builderImpl) init(base *Version, cmp util.Comparator) {
  b.cmp = cmp
  b.files = make([]map[int]*table.FileMetaData, util.Global.MaxLevel)
  for i := 0; i < util.Global.MaxLevel; i++ {
    b.files[i] = make(map[int]*table.FileMetaData)

    for _, file := range base.files[i] {
      b.files[i][file.Number] = file
    }
  }
}

func (b *builderImpl) Apply(edit *VersionEdit) {
  for _, del := range edit.Deletes {
    level := del.level
    num   := del.value.(int)
    delete(b.files[level], num)
  }
  
  for _, add := range edit.Files {
    level := add.level
    meta  := add.value.(*table.FileMetaData)
    b.files[level][meta.Number] = meta
  }
}

func (b *builderImpl) Finish(v *Version) {
  v.files = make([][]*table.FileMetaData, util.Global.MaxLevel)
  for i := 0; i < len(b.files); i++ {
    tmp := make([]interface{}, len(b.files[i]))
    cnt := 0
    for _, meta := range b.files[i] {
      tmp[cnt] = meta
      cnt++
    }
    sort.Sort(util.NewSliceSorter(tmp, b.metaCompare))
    
    for _, meta := range tmp {
      v.files[i] = append(v.files[i], meta.(*table.FileMetaData))
    }
  }
}


func (b *builderImpl) metaCompare(first, second interface{}) int {
  fmeta := first.(*table.FileMetaData)
  smeta := second.(*table.FileMetaData)
  if rslt := b.cmp.Compare(fmeta.Smallest.Encode(), smeta.Smallest.Encode()); rslt == 0 {
    return smeta.Number - fmeta.Number
  } else {
    return rslt
  }
}
