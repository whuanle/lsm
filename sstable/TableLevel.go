package sstable

/*
哈希表用于实现多层 SsTable
*/

//
type level struct {
	key   int
	value *SsTable
	next  *level
}

// TableLevel 哈希表
type TableLevel struct {
	levels []*level
	length int
}

func Init(n int) *TableLevel {
	return &TableLevel{
		levels: make([]*level, n),
		length: n,
	}
}

//
//// Insert 插入一个 SsTable
//func (hash *SsTableLevel) Insert(level int, table *SsTable) (bool, string) {
//	index := hash.GetHashCode(key)
//
//	// 位置还没有元素
//	if hash.table[index] == nil {
//		hash.table[index] = &level{
//			key:   key,
//			value: value,
//		}
//	} else {
//		node := hash.table[index]
//		for {
//			// 如果 key 相同，则覆盖，并返回旧值
//			if node.key == key {
//				tmp := node.value
//				node.value = value
//				return true, tmp
//			}
//			// 可以插入元素
//			if node.next == nil {
//				node.next = &level{
//					key:   key,
//					value: value,
//				}
//				return false, ""
//			}
//			// 下一次检索
//			node = node.next
//		}
//	}
//	return false, ""
//}
//
//// Get 获取一个元素
//func (hash *SsTableLevel) Get(key string) (bool, string) {
//	index := hash.GetHashCode(key)
//	if hash.table[index] == nil {
//		return false, ""
//	}
//
//	node := hash.table[index]
//	for {
//		if node.key == key {
//			return true, node.value
//		}
//		if node.next == nil {
//			return false, ""
//		}
//		node = node.next
//	}
//}
