package Memory

import (
	"github.com/whuanle/lsm/Value"
	"log"
)

// sortTreeNode 有序树节点
type sortTreeNode struct {
	KV    Value.KVData
	Left  *sortTreeNode
	Right *sortTreeNode
}

// SortTree 有序树
type SortTree struct {
	root  *sortTreeNode
	count int
}

func (tree *SortTree) GetCount() int {
	return tree.count
}

// Search 查找 Key 的值
func (tree *SortTree) Search(key string) (Value.KVData, bool) {
	if tree == nil {
		log.Fatal("树为空")
	}
	current := tree.root
	// 层次遍历
	for current != nil {
		if key == current.KV.Key && current.KV.Deleted == false {
			return current.KV, true
		}

		if key < current.KV.Key {
			// 继续对比下一层
			current = current.Left
		} else {
			// 继续对比下一层
			current = current.Right
		}
	}
	return Value.KVData{}, false
}

// Insert 插入元素
func (tree *SortTree) Insert(key string, value []byte) bool {
	if tree == nil {
		log.Fatal()
	}

	newNode := &sortTreeNode{
		KV: Value.KVData{
			Key:   key,
			Value: value,
		},
	}

	if tree.root == nil {
		tree.root = newNode
		tree.count++
		return true
	}

	current := tree.root

	for current != nil {
		// 如果已经存在键，则替换值
		if key == current.KV.Key && current.KV.Deleted == false {
			current.KV.Value = value
			return true
		}
		// 要插入左边
		if key < current.KV.Key {
			// 左孩为空，直接插入左边
			if current.Left == nil {
				current.Left = newNode
				tree.count++
				return true
			}
			// 继续对比下一层
			current = current.Left
		} else {
			if current.Right == nil {
				current.Right = newNode
				tree.count++
				return true
			}
			// 继续对比下一层
			current = current.Right
		}
	}
	tree.count++
	return true
}

// Delete 删除并返回旧值
func (tree *SortTree) Delete(key string) (Value.KVData, bool) {
	if tree == nil {
		log.Fatal()
	}
	current := tree.root
	for current != nil {
		if key == current.KV.Key && current.KV.Deleted == false {
			current.KV.Deleted = true
			tree.count--
			return current.KV, true
		}

		if key < current.KV.Key {
			// 继续对比下一层
			current = current.Left
		} else {
			// 继续对比下一层
			current = current.Right
		}
	}
	return Value.KVData{}, false
}
