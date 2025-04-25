package ssTable

import (
	"LSM/kv"
	"fmt"
	"sync"
)

type TableTree struct {
	levels     []*tableNode
	lock       *sync.RWMutex
	levellocks []*sync.RWMutex
}

type tableNode struct {
	index        int
	table        *SSTable
	next         *tableNode
	tableNodeL   string
	tableNodeR   string
	needToDelete bool
}

func (tree *TableTree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	for level, node := range tree.levels {
		tables := make([]*tableNode, 0)
		for node != nil {
			tables = append(tables, node)
			node = node.next
		}
		if level == 0 {
			for i := len(tables) - 1; i >= 0; i-- {
				value, searchResult := tables[i].table.Search(key)
				if searchResult == kv.NotFind {
					continue
				}
				return value, searchResult
			}
		} else {
			for i := 0; i < len(tables); i++ {
				if tables[i].tableNodeL > key || tables[i].tableNodeR < key {
					continue
				}
				value, searchResult := tables[i].table.Search(key)
				if searchResult == kv.NotFind {
					continue
				}
				return value, searchResult
			}
		}
	}
	return kv.Value{}, kv.NotFind
}
func (tree *TableTree) Insert(table *SSTable, level int) (index int) {
	node := tree.levels[level]
	newNode := &tableNode{
		table:        table,
		next:         nil,
		index:        0,
		tableNodeL:   table.sortIndex[0],
		tableNodeR:   table.sortIndex[len(table.sortIndex)-1],
		needToDelete: false,
	}
	if node == nil {
		tree.levels[level] = newNode
	} else {
		for node != nil {
			if node.next == nil {
				node.next = newNode
				newNode.index = node.index + 1
				break
			}
			node = node.next
		}
	}
	return newNode.index
}
func (tree *TableTree) getMaxIndex(level int) int {
	node := tree.levels[level]
	index := 0
	for node != nil {
		index = node.index
		node = node.next
	}
	return index
}
func (tree *TableTree) getLevelCount(level int) int {
	node := tree.levels[level]
	count := 0
	for node != nil {
		count++
		node = node.next
	}
	return count
}
func getLevel(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n != 2 && err != nil {
		return 0, 0, fmt.Errorf("incorrect data file name: %q", name)
	}
	return level, index, nil
}
