package sstable

import "github.com/whuanle/lsm/config"

var levelMaxSize []int

func init() {
	con := config.GetConfig()
	levelMaxSize = make([]int, 10)
	levelMaxSize[0] = con.Level0
	levelMaxSize[1] = levelMaxSize[0] * 10
	levelMaxSize[2] = levelMaxSize[1] * 10
	levelMaxSize[3] = levelMaxSize[2] * 10
	levelMaxSize[4] = levelMaxSize[3] * 10
	levelMaxSize[5] = levelMaxSize[4] * 10
	levelMaxSize[6] = levelMaxSize[5] * 10
	levelMaxSize[7] = levelMaxSize[6] * 10
	levelMaxSize[8] = levelMaxSize[7] * 10
	levelMaxSize[9] = levelMaxSize[8] * 10
}
