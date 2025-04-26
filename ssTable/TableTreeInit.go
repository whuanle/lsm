package ssTable

import (
	"LSM/config"
	"io/ioutil"
	"log"
	"path"
	"sync"
	"time"
)

var levelMaxSize []int

func (tree *TableTree) Init(dir string) {
	log.Println("The SSTable list are being loaded")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("The SSTable list are being loaded,consumption of time : ", elapse)
	}()
	con := config.GetConfig()
	levelMaxSize = make([]int, 10)
	levelMaxSize[0] = con.Level0Size
	levelMaxSize[1] = con.Level0Size * 10
	levelMaxSize[2] = levelMaxSize[1] * 10
	levelMaxSize[3] = levelMaxSize[2] * 10
	levelMaxSize[4] = levelMaxSize[3] * 10
	levelMaxSize[5] = levelMaxSize[4] * 10
	levelMaxSize[6] = levelMaxSize[5] * 10
	levelMaxSize[7] = levelMaxSize[6] * 10
	levelMaxSize[8] = levelMaxSize[7] * 10
	levelMaxSize[9] = levelMaxSize[8] * 10
	tree.levels = make([]*tableNode, 10)
	tree.lock = &sync.RWMutex{}
	tree.levellocks = make([]*sync.RWMutex, 10)
	for i := 0; i < 10; i++ {
		tree.levellocks[i] = &sync.RWMutex{}
	}
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("Failed to read the database file")
		panic(err)
	}
	for _, info := range infos {
		if path.Ext(info.Name()) == ".db" {
			tree.loadDbFile(path.Join(dir, info.Name()))
		}
	}
}
