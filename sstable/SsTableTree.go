package sstable

/*
哈希表用于实现多层 SsTable
*/

// 每一层的 SsTable 表
type levelNode struct {
	table *SsTable
	next  *levelNode
}

// SsTableTree 树
type SsTableTree struct {
	levels []*levelNode
	length int
}

// Init 初始化 SsTableTree
func Init(n int) *SsTableTree {
	return &SsTableTree{
		levels: make([]*levelNode, n),
		length: n,
	}
}

// Search 从所有 SsTable 表中查找数据
func (level *SsTableTree) Search(key string) (KV.KVData, bool) {
	for _, v := range level.levels {
		node := v
		for node != nil {
			kv, success := node.table.Query(key)
			if success {
				// 如果已经被删除，则不需要再向下查找
				if kv.Deleted == false {
					return KV.KVData{}, false
				}
				return kv, success
			}
			node = v.next
		}
	}
	return KV.KVData{}, false
}

// Insert 插入一个 SsTable
func (level *SsTableTree) Insert(table *SsTable) {
	// 位置还没有元素
	if level.levels[0] == nil {
		level.levels[0] = &levelNode{
			table: table,
		}
	} else {
		node := level.levels[0]
		for node != nil {
			if node.next == nil {
				node.next = &levelNode{
					table: table,
				}
			}
			node = node.next
		}
	}
}
