package sstable

import (
	"encoding/json"
	"fmt"
	"github.com/whuanle/lsm/config"
	"github.com/whuanle/lsm/kv"
	"github.com/whuanle/lsm/memory"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

/*
负责管理所有 SsTable，判断是否需要合并文件到下一层
*/

// 链表，表示每一层的 SsTable
type tableNode struct {
	index int
	table *SsTable
	next  *tableNode
}

// TableTree 树
type TableTree struct {
	levels []*tableNode
	length int
	lock   sync.RWMutex
}

// Init 初始化 TableTree
func Init(n int) *TableTree {
	return &TableTree{
		levels: make([]*tableNode, n),
		length: n,
	}
}

// 后面使用队列做查询队列，不使用锁

// Search 从所有 SsTable 表中查找数据
func (tree *TableTree) Search(key string) (kv.Value, bool) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	for _, v := range tree.levels {
		node := v
		for node != nil {
			data, success := node.table.Query(key)
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
	node := tree.levels[0]
	for node != nil {
		if node.next == nil {
			return node.index
		}
		node = node.next
	}
	return 0
}
func (tree *TableTree) getCount(level int) int {
	node := tree.levels[0]
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

// 压缩文件
func (tree *TableTree) majorCompaction() {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	con := config.GetConfig()
	for k, v := range tree.levels {
		// 当前层 SsTable 数量是否已经到达阈值
		if tree.getCount(k) > con.PartSize {
			tree.majorCompactionLevel(k)
			continue
		}
		// 当前层的 SsTable 总大小已经到底阈值
		var size int64
		node := v
		for node != nil {
			size += node.table.GetDbSize()
		}
		tableSize := int(size / 1000 / 1000)
		if tableSize > levelMaxSize[k] {
			tree.majorCompactionLevel(k)
			continue
		}
	}
}

// 压缩当前层的文件到下一层
func (tree *TableTree) majorCompactionLevel(level int) {
	// 用于加载 SsTable 的内存缓
	currentCache := make([]byte, levelMaxSize[level])
	node := tree.levels[level]
	sortTree := memory.SortTree{}

	for node != nil {
		table := node.table
		// 将 SsTable 的数据区加载到 currentCache 内存中
		length := table.tableMetaInfo.dataLen
		data := currentCache[0:length]
		table.readDataArea(data)
		// 读取每一个元素
		for k, v := range table.sparseIndex {
			if v.Deleted == false {
				sortTree.Insert(k, data[v.Start:(v.Start+v.Len)])
			} else {
				sortTree.Delete(k)
			}
		}
		node = node.next
	}

	// 将 SortTree 压缩合并成一个 SsTable
	// key 有序的元素集合
	values := sortTree.GetValues()
	newLevel := level
	// 目前最多支持 10 层
	if newLevel > 10 {
		newLevel = 10
	}
	// 保存到下一层
	tree.Save(values, newLevel)
	// 清理该层的文件
	oldTable := tree.levels[level]
	tree.levels[level] = nil
	for oldTable != nil {
		oldTable.table.Clear()
		oldTable.table = nil
		oldTable = oldTable.next
	}
}

// LoadDbFile 加载一个 db 文件
func (tree *TableTree) LoadDbFile(path string) {
	defer func() {
		if r := recover(); r != nil {
			log.Println(r)
		}
	}()
	level, index := getLevel(filepath.Base(path))
	table := &SsTable{
		filePath: path,
	}
	node := &tableNode{
		index: index,
		table: table,
	}
	tree.insertNode(node, level)
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
