package version

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
  files   []map[int]*table.FileMetaData
}

func NewVersionBuilder(base *Version) Builder {
  builder := new(builderImpl)
  builder.init(base)
  return builder
}

func (b *builderImpl) init(base *Version) {
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
  for i, files := range b.files {

    for _, f := range files {
      v.files[i] = append(v.files[i], f)
    }
  }
}
