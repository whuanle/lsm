package ssTable

import (
	"encoding/json"
	"github.com/whuanle/lsm/config"
	"github.com/whuanle/lsm/kv"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
)

// CreateNewTable 创建新的 SSTable
func (tree *TableTree) CreateNewTable(values []kv.Value) {
	tree.createTable(values, 0)
}

// 创建新的 SSTable，插入到合适的层
func (tree *TableTree) createTable(values []kv.Value, level int) *SSTable {
	// 生成数据区
	keys := make([]string, 0, len(values))
	positions := make(map[string]Position)
	dataArea := make([]byte, 0)
	for _, value := range values {
		data, err := kv.Encode(value)
		if err != nil {
			log.Println("Failed to insert Key: ", value.Key, err)
			continue
		}
		keys = append(keys, value.Key)
		// 文件定位记录
		positions[value.Key] = Position{
			Start:   int64(len(dataArea)),
			Len:     int64(len(data)),
			Deleted: value.Deleted,
		}
		dataArea = append(dataArea, data...)
	}
	sort.Strings(keys)

	// 生成稀疏索引区
	// map[string]Position to json
	indexArea, err := json.Marshal(positions)
	if err != nil {
		log.Fatal("An SSTable file cannot be created,", err)
	}

	// 生成 MetaInfo
	meta := MetaInfo{
		version:    0,
		dataStart:  0,
		dataLen:    int64(len(dataArea)),
		indexStart: int64(len(dataArea)),
		indexLen:   int64(len(indexArea)),
	}

	table := &SSTable{
		tableMetaInfo: meta,
		sparseIndex:   positions,
		sortIndex:     keys,
		lock:          &sync.RWMutex{},
	}
	index := tree.insert(table, level)
	log.Printf("Create a new SSTable,level: %d ,index: %d\r\n", level, index)
	con := config.GetConfig()
	filePath := con.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	table.filePath = filePath

	writeDataToFile(filePath, dataArea, indexArea, meta)
	// 以只读的形式打开文件
	f, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
	if err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	table.f = f

	return table
}
