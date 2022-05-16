package ssTable

import (
	"encoding/json"
	"fmt"
	"github.com/whuanle/lsm/config"
	"github.com/whuanle/lsm/kv"
	"log"
	"sort"
	"strconv"
	"strings"
)

// Init 初始化 TableTree
func Init(n int) *TableTree {
	return &TableTree{
		levels: make([]*tableNode, n),
	}
}

// Search 从所有 SsTable 表中查找数据
func (tree *TableTree) Search(key string) (kv.Value, bool) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	// 查找的时候要从最后一个 SsTable 开始查找
	for _, node := range tree.levels {
		for node != nil {
			data, success := node.table.Search(key)
			if success {
				// 如果已经被删除，则不需要再向下查找
				if data.Deleted == false {
					return kv.Value{}, false
				}
				return data, success
			}
			node = node.next
		}
	}
	return kv.Value{}, false
}

// 获取一层中的 SsTable 的最大序号
func (tree *TableTree) getMaxIndex(level int) int {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	node := tree.levels[level]
	for node != nil {
		if node.next == nil {
			return node.index
		}
		node = node.next
	}
	return 0
}

// 获取该层有多少个 SsTable
func (tree *TableTree) getCount(level int) int {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	node := tree.levels[level]
	count := 0
	for node != nil {
		count++
		node = node.next
	}
	return count
}

// Save 将元素保存到 SsTable 中
func (tree *TableTree) Save(values []kv.Value, level int) {
	// 生成数据区，新的数据在文件前面，旧的数据在后面
	index := 0
	keys := make([]string, 0)
	position := make(map[string]Position)
	dataArea := make([]byte, 0)
	for _, v := range values {
		data, err := json.Marshal(v)
		if err != nil {
			log.Fatal(err)
		}
		keys = append(keys, v.Key)
		dataArea = append(dataArea, data...)
		position[v.Key] = Position{
			Start:   int64(index),
			Len:     int64(len(data)),
			Deleted: v.Deleted,
		}
		index += len(dataArea)
	}
	sort.Strings(keys)

	// 生成稀疏索引区
	indexArea, err := json.Marshal(position)
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
	// 记录到 SsTable
	tree.lock.Lock()
	defer tree.lock.Unlock()

	con := config.GetConfig()
	maxIndex := tree.getMaxIndex(0)
	filePath := con.DataDir + "/" + "0." + strconv.Itoa(maxIndex) + ".db"

	table := SsTable{
		filePath:      filePath,
		tableMetaInfo: meta,
		sparseIndex:   position,
		sortIndex:     keys,
	}
	tree.insert(&table, level)
	table.saveToFile(dataArea, indexArea, meta)
}

// 插入一个 SsTable
func (tree *TableTree) insert(table *SsTable, level int) {
	// 每次插入的，都出现在最后面
	maxIndex := tree.getMaxIndex(level)
	node := tree.levels[level]
	newNode := &tableNode{
		table: table,
		next:  node,
		index: maxIndex,
	}

	if node == nil {
		tree.levels[0] = newNode
	} else {
		i := 0
		for node != nil {
			i++
			if node.next == nil {
				node.next = newNode
				break
			}
		}
	}
}

func (tree *TableTree) insertNode(newNode *tableNode, level int) {
	currentNode := tree.levels[level]
	if currentNode == nil {
		tree.levels[level] = newNode
		return
	}
	// 将 SsTable 插入到合适的位置
	for currentNode != nil {
		if newNode.index > currentNode.index {
			if currentNode.next == nil || currentNode.next.index > newNode.index {
				newNode.next = currentNode.next
				currentNode.next = newNode
				break
			}
		} else {
			newNode.next = currentNode
			tree.levels[level] = newNode
			break
		}
	}
}

// 获取一个 db 文件所代表的 SsTable 的所在层数和索引
func getLevel(name string) (level int, index int) {
	// 0.1.db
	strs := strings.Split(name, ".")
	if len(strs) != 3 {
		panic(fmt.Sprint("Incorrect data file name:", name))
	}
	tmp, err := strconv.ParseInt(strs[0], 10, 64)
	if err != nil {
		panic(fmt.Sprint("Incorrect data file name:", name))
	}
	level = int(tmp)
	tmp, err = strconv.ParseInt(strs[1], 10, 64)
	if err != nil {
		panic(fmt.Sprint("Incorrect data file name:", name))
	}
	index = int(tmp)
	return level, index
}
