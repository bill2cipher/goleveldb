package util

type Config struct {
  kL0_StopWritesTrigger int
  Max_Level int
}

var Global Config

func init() {
  Global.kL0_StopWritesTrigger = 1024 * 1024 * 4
  Global.Max_Level = 7
}
