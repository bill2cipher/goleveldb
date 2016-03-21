package compact

import (
  "github.com/goleveldb/db"
)

type Compact interface {
  // Start the background compaction routine
  Start(db db.DB) bool
  
  // Wait for imm or l0 compaction to be done
  Wait4Compact()
}

type compactImpl struct {
  
}

func NewCompact() Compact {
  c := new(compactImpl)
  c.init()
  return c
}


func (c *compactImpl) Start() {
  
}

func (c *compactImpl) Wait4Compact() {
  
}