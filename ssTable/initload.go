package ssTable

import (
	"log"
	"path/filepath"
	"time"
)

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
	newNode := &tableNode{
		index: index,
		table: table,
	}

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
