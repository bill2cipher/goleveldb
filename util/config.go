package util

type Config interface {
  L0_StopWritesTrigger() int
}

var Global config

type config struct {
  kL0_StopWritesTrigger int
}

func init() {
  Global.kL0_StopWritesTrigger = 1024 * 1024 * 4
}

func (c config) L0_StopWritesTrigger() int {
  return c.kL0_StopWritesTrigger
}
