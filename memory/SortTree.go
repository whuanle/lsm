package memory

import (
	"github.com/whuanle/lsm/kv"
	"log"
)

// sortTreeNode 有序树节点
type sortTreeNode struct {
	KV    kv.Value
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
func (tree *SortTree) Search(key string) (kv.Value, bool) {
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
	return kv.Value{}, false
}

// Insert 插入元素
func (tree *SortTree) Insert(key string, value []byte) bool {
	if tree == nil {
		log.Fatal()
	}

	newNode := &sortTreeNode{
		KV: kv.Value{
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
		if key == current.KV.Key {
			current.KV.Value = value
			current.KV.Deleted = false
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
func (tree *SortTree) Delete(key string) (kv.Value, bool) {
	if tree == nil {
		log.Fatal()
	}

	newNode := &sortTreeNode{
		KV: kv.Value{
			Key:     key,
			Value:   nil,
			Deleted: true,
		},
	}

	current := tree.root
	for current != nil {
		if key == current.KV.Key {
			if current.KV.Deleted == false {
				tree.count--
			}
			current.KV.Deleted = true
			return current.KV, true
		}
		// 如果不存在此 key，则插入一个删除标记，因为此 key 可能在 SsTable 中
		if key < current.KV.Key {
			// 继续对比下一层
			// 左孩为空，直接插入左边
			if current.Left == nil {
				current.Left = newNode
			}
			// 继续对比下一层
			current = current.Left
		} else {
			if current.Right == nil {
				current.Right = newNode
			}
			// 继续对比下一层
			current = current.Right
		}
	}
	return kv.Value{}, false
}

// GetValues 获取有序元素列表
func (tree *SortTree) GetValues() []kv.Value {
	stack := &Stack{
		stack:  make([]*sortTreeNode, tree.count),
		length: tree.count,
	}
	values := make([]kv.Value, 0)
	// 使用栈非递归遍历树
	node := tree.root
	for node != nil {
		if node != nil {
			values = append(values, node.KV)
			stack.Push(node)
			node = node.Left
		} else {
			node, success := stack.Pop()
			if success == false {
				break
			}
			node = node.Right
		}
	}
	return values
}
