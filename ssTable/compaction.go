package ssTable

import (
	"github.com/whuanle/lsm/config"
	"github.com/whuanle/lsm/memory"
)

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
				sortTree.Set(k, data[v.Start:(v.Start+v.Len)])
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
