package ssTable

import (
	"github.com/whuanle/lsm/config"
	"io/ioutil"
	"log"
	"path"
	"sync"
	"time"
)

var levelMaxSize []int

// Init 初始化 TableTree
func (tree *TableTree) Init(dir string) {
	log.Println("The SSTable list are being loaded")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("The SSTable list are being loaded,consumption of time : ", elapse)
	}()

	// 初始化每一层 SSTable 的文件总最大值
	con := config.GetConfig()
	levelMaxSize = make([]int, 10)
	levelMaxSize[0] = con.Level0Size
	levelMaxSize[1] = levelMaxSize[0] * 10
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
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Println("Failed to read the database file")
		panic(err)
	}
	for _, info := range infos {
		// 如果是 SSTable 文件
		if path.Ext(info.Name()) == ".db" {
			tree.loadDbFile(path.Join(dir, info.Name()))
		}
	}
}
