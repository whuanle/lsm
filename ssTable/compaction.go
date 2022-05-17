package ssTable

import (
	"github.com/whuanle/lsm/config"
	"github.com/whuanle/lsm/kv"
	"github.com/whuanle/lsm/sortTree"
	"log"
)

/*
TableTree 检查是否需要压缩 SsTable
*/

// Check 检查是否需要压缩数据库文件
func (tree *TableTree) Check() {
	tree.majorCompaction()
}

// 压缩文件
func (tree *TableTree) majorCompaction() {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	con := config.GetConfig()
	for levelIndex, levelNode := range tree.levels {
		// 当前层 SsTable 数量是否已经到达阈值
		if tree.getCount(levelIndex) > con.PartSize {
			tree.majorCompactionLevel(levelIndex)
			continue
		}
		// 当前层的 SsTable 总大小已经到底阈值
		var size int64
		node := levelNode
		for node != nil {
			size += node.table.GetDbSize()
		}
		tableSize := int(size / 1000 / 1000)
		if tableSize > levelMaxSize[levelIndex] {
			tree.majorCompactionLevel(levelIndex)
			continue
		}
	}
}

// 压缩当前层的文件到下一层
func (tree *TableTree) majorCompactionLevel(level int) {
	// 用于加载 一个 SsTable 的内容到缓存中
	tableCache := make([]byte, levelMaxSize[level])
	currentNode := tree.levels[level]
	// 将当前层的 SsTable 合并到一个有序二叉树中
	memoryTree := &sortTree.Tree{}

	for currentNode != nil {
		table := currentNode.table
		// 将 SsTable 的数据区加载到 tableCache 内存中
		if int64(len(tableCache)) < table.tableMetaInfo.dataLen {
			tableCache = make([]byte, table.tableMetaInfo.dataLen)
		}
		newSlice := tableCache[0:table.tableMetaInfo.dataLen]
		table.readDataArea(newSlice)
		// 读取每一个元素
		for k, position := range table.sparseIndex {
			if position.Deleted == false {
				value, err := kv.Decode(newSlice[position.Start:(position.Start + position.Len)])
				if err != nil {
					log.Fatal(err)
				}
				memoryTree.Set(k, value.Value)
			} else {
				memoryTree.Delete(k)
			}
		}
		currentNode = currentNode.next
	}

	// 将 SortTree 压缩合并成一个 SsTable
	values := memoryTree.GetValues()
	newLevel := level
	// 目前最多支持 10 层
	if newLevel > 10 {
		newLevel = 10
	}
	// 创建新的 SsTable
	tree.createTable(values, newLevel)
	// 清理该层的文件
	oldNode := tree.levels[level]
	// 重置该层
	if level < 10 {
		tree.levels[level] = nil
	}
	// 清理每个旧的 SsTable
	for oldNode != nil {
		oldNode.table.Delete()
		oldNode.table = nil
		oldNode = oldNode.next
	}
}
