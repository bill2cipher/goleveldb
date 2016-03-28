package compact

import (
	"github.com/jellybean4/goleveldb/table"
	"github.com/jellybean4/goleveldb/util"
)

type Compact struct {
  Files  [][]*table.FileMetaData
  Level  int
  Smallest *util.InternalKey
  Largest *util.InternalKey
}

func (c *Compact) init() {
  c.Files = make([][]*table.FileMetaData, 2)
}


func NewCompact() *Compact {
  comp := new(Compact)
  comp.init()
  return comp
}
