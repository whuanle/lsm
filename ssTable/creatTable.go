package ssTable

import (
	"LSM/config"
	"LSM/kv"
	"encoding/json"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
)

func (tree *TableTree) CreatNewTable(values []kv.Value) {
	tree.levellocks[0].Lock()
	tree.creatTable(values, 0)
	tree.levellocks[0].Unlock()
}
func (tree *TableTree) creatTable(values []kv.Value, level int) *SSTable {
	//println("creatTable---------------\n", level)
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
		positions[value.Key] = Position{
			Start:   int64(len(dataArea)),
			Len:     int64(len(data)),
			Deleted: value.Delete,
		}
		dataArea = append(dataArea, data...)
	}
	sort.Strings(keys)

	indexArea, err := json.Marshal(positions)
	if err != nil {
		log.Fatal("An SSTable file cannot be created,", err)
	}
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
	index := tree.Insert(table, level)
	//log.Printf("Create a new SSTable,level: %d ,index: %d\r\n", level, index)
	con := config.GetConfig()
	filePath := con.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	table.filePath = filePath
	writeDataToFile(filePath, dataArea, indexArea, meta)
	f, err := os.OpenFile(table.filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("Failed to open new SSTable,", err)
		panic(err)
	}
	table.f = f
	return table
}
