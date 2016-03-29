package compact

import (
  "bytes"
  "fmt"
)

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

func (c *Compact) Dump() string {
  var buffer bytes.Buffer
  for i := 0; i < 2; i++ {
    buffer.WriteString(fmt.Sprintf("Compacting Files Level : %d : [", c.Level + i))
    for _, meta := range c.Files[i] {
      buffer.WriteString(fmt.Sprintf("%d, ", meta.Number))
    }
    buffer.WriteString("]    ")
  }
  return buffer.String()
}

func NewCompact() *Compact {
  comp := new(Compact)
  comp.init()
  return comp
}
