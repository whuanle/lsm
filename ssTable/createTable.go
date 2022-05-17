package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"github.com/whuanle/lsm/config"
	"github.com/whuanle/lsm/kv"
	"log"
	"os"
	"sort"
	"strconv"
)

// CreateNewTable 创建新的 SsTable
func (tree *TableTree) CreateNewTable(values []kv.Value) {
	tree.createTable(values, 0)
}

// 创建新的 SsTable，插入到合适的层
func (tree *TableTree) createTable(values []kv.Value, level int) {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	// 生成数据区
	index := 0
	keys := make([]string, 0)
	positions := make(map[string]Position)
	dataArea := make([]byte, 0)
	for _, v := range values {
		data, err := json.Marshal(v)
		if err != nil {
			log.Fatal(err)
		}
		keys = append(keys, v.Key)
		dataArea = append(dataArea, data...)
		positions[v.Key] = Position{
			Start:   int64(index),
			Len:     int64(len(data)),
			Deleted: v.Deleted,
		}
		index += len(dataArea)
	}
	sort.Strings(keys)

	// 生成稀疏索引区
	// map[string]Position to json
	indexArea, err := json.Marshal(positions)
	if err != nil {
		log.Fatal(err)
	}

	// 生成 MetaInfo
	meta := MetaInfo{
		version:    0,
		dataStart:  0,
		dataLen:    int64(len(dataArea)),
		indexStart: int64(len(dataArea)),
		indexLen:   int64(len(indexArea)),
	}

	con := config.GetConfig()
	maxIndex := tree.getMaxIndex(0)
	filePath := con.DataDir + "/" + "0." + strconv.Itoa(maxIndex) + ".db"

	table := &SsTable{
		filePath:      filePath,
		tableMetaInfo: meta,
		sparseIndex:   positions,
		sortIndex:     keys,
	}

	f, err := os.OpenFile(table.filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(" error create file ", err)
	}
	_, err = f.Write(dataArea)
	if err != nil {
		log.Fatal(" error write file ", err)
	}
	_, err = f.Write(indexArea)
	if err != nil {
		log.Fatal(" error write file ", err)
	}
	// 写入元数据到文件末尾
	// 注意，右侧必须能够识别字节长度的类型，不能使用 int 这种类型，只能使用 int32、int64 等
	_ = binary.Write(f, binary.LittleEndian, &meta.version)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataLen)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexLen)
	_, _ = f.Seek(0, 0)

	tree.insert(table, level)
}

// 插入一个 SsTable 到指定层
func (tree *TableTree) insert(table *SsTable, level int) {
	// 每次插入的，都出现在最后面
	node := tree.levels[level]
	newNode := &tableNode{
		table: table,
		next:  node,
	}

	if node == nil {
		tree.levels[0] = newNode
	} else {
		i := 0
		for node != nil {
			i++
			if node.next == nil {
				newNode.index = node.index + 1
				node.next = newNode
				break
			}
		}
	}
}
