package ssTable

import (
	"fmt"
	"github.com/whuanle/lsm/kv"
	"sync"
)

// TableTree 树
type TableTree struct {
	levels []*tableNode
	// 用于避免进行插入或压缩、删除 SSTable 时发生冲突
	lock *sync.RWMutex
}

// 链表，表示每一层的 SSTable
type tableNode struct {
	index int
	table *SSTable
	next  *tableNode
}

// Search 从所有 SSTable 表中查找数据
func (tree *TableTree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	// 遍历每一层的 SSTable
	for _, node := range tree.levels {
		// 整理 SSTable 列表
		tables := make([]*SSTable, 0)
		for node != nil {
			tables = append(tables, node.table)
			node = node.next
		}
		// 查找的时候要从最后一个 SSTable 开始查找
		for i := len(tables) - 1; i >= 0; i-- {
			value, searchResult := tables[i].Search(key)
			// 未找到，则查找下一个 SSTable 表
			if searchResult == kv.None {
				continue
			} else { // 如果找到或已被删除，则返回结果
				return value, searchResult
			}
		}
	}
	return kv.Value{}, kv.None
}

// 获取一层中的 SSTable 的最大序号
func (tree *TableTree) getMaxIndex(level int) int {
	node := tree.levels[level]
	index := 0
	for node != nil {
		index = node.index
		node = node.next
	}
	return index
}

// 获取该层有多少个 SSTable
func (tree *TableTree) getCount(level int) int {
	node := tree.levels[level]
	count := 0
	for node != nil {
		count++
		node = node.next
	}
	return count
}

// 获取一个 db 文件所代表的 SSTable 的所在层数和索引
func getLevel(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("incorrect data file name: %q", name)
	}
	return level, index, nil
}
