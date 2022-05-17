package ssTable

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

/*
管理 SsTable 的磁盘文件
*/

// GetDbSize 获取 .db 数据文件大小
func (table *SsTable) GetDbSize() int64 {
	info, err := os.Stat(table.filePath)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}

// 加载一个 db 文件到 TableTree 中
func (tree *TableTree) loadDbFile(path string) {
	log.Println("Loading the ", path)
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Consumption of time : ", elapse)
	}()

	level, index := getLevel(filepath.Base(path))
	table := &SsTable{}
	table.Init(path)
	node := &tableNode{
		index: index,
		table: table,
	}
	tree.insertNode(node, level)
}

// 插入节点到 TableTree 中
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

// 加载稀疏索引区到内存
func (table *SsTable) loadSparseIndex() {
	// 加载稀疏索引区
	bytes := make([]byte, table.tableMetaInfo.indexLen)
	if _, err := table.f.Seek(table.tableMetaInfo.indexStart, 0); err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	if _, err := table.f.Read(bytes); err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}

	// 反序列化到内存
	table.sparseIndex = make(map[string]Position)
	err := json.Unmarshal(bytes, &table.sparseIndex)
	if err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	_, _ = table.f.Seek(0, 0)

	// 先排序
	var keys []string
	for k := range table.sparseIndex {
		if table.sparseIndex[k].Deleted {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	table.sortIndex = keys
}

// 加载文件句柄
func (table *SsTable) loadFileHandle() {
	if table.f == nil {
		// 以只读的形式打开文件
		f, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
		if err != nil {
			log.Println(" error open file ", table.filePath)
			panic(err)
		}

		table.f = f
	}
	// 加载文件句柄的同时，加载表的元数据
	table.loadMetaInfo()
	table.loadSparseIndex()
}

// 加载 SsTable 文件的元数据，从 SsTable 磁盘文件中读取出 TableMetaInfo
func (table *SsTable) loadMetaInfo() {
	f := table.f
	_, err := f.Seek(0, 0)
	if err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}

}

// 读取 SsTable 的数据区
func (table *SsTable) readDataArea(data []byte) {
	table.lock.Lock()
	defer table.lock.Unlock()
	defer table.f.Seek(0, 0)
	if _, err := table.f.Seek(0, 0); err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}

	if _, err := table.f.Read(data); err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
}

// Delete 删除一个 Table
func (table *SsTable) Delete() {
	table.lock.Lock()
	defer table.lock.Unlock()

	err := table.f.Close()
	if err != nil {
		log.Println(" error close file ", table.filePath)
		panic(err)
	}
	err = os.Remove(table.filePath)
	if err != nil {
		log.Println(" error delete file ", table.filePath)
		panic(err)
	}
	table.f = nil
}
