package compact

type Compaction interface {
  // Start the background compaction routine
  Start() error
  
  // Wait for imm or l0 compaction to be done
  Wait4Compact()
}