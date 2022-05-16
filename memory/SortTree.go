package memory

import (
	"github.com/whuanle/lsm/kv"
	"log"
	"sync"
)

// sortTreeNode 有序树节点
type sortTreeNode struct {
	KV    kv.Value
	Left  *sortTreeNode
	Right *sortTreeNode
}

// SortTree 有序树
type SortTree struct {
	root   *sortTreeNode
	count  int
	rWLock *sync.RWMutex
}

func (tree *SortTree) Init() {
	tree.rWLock = &sync.RWMutex{}
}

// GetCount 获取内存表元素数量
func (tree *SortTree) GetCount() int {
	return tree.count
}

// Search 查找 Key 的值，
// kv.Value 的 Deleted 一定为 false
func (tree *SortTree) Search(key string) (kv.Value, kv.SearchResult) {
	if tree == nil {
		log.Fatal("The tree is nil")
	}

	tree.rWLock.RLock()
	defer tree.rWLock.RUnlock()

	currentNode := tree.root
	// 层次遍历
	for currentNode != nil {
		if key == currentNode.KV.Key {
			if currentNode.KV.Deleted == false {
				return currentNode.KV, kv.Success
			} else {
				return kv.Value{}, kv.Deleted
			}
		}
		if key < currentNode.KV.Key {
			// 继续对比下一层
			currentNode = currentNode.Left
		} else {
			// 继续对比下一层
			currentNode = currentNode.Right
		}
	}
	return kv.Value{}, kv.None
}

// Set 设置 Key 的值并返回旧值，
// 返回的 bool 只表示是否有旧值
func (tree *SortTree) Set(key string, value []byte) (oldValue kv.Value, hasOld bool) {
	if tree == nil {
		log.Fatal("The tree is nil")
	}

	current := tree.root
	newNode := &sortTreeNode{
		KV: kv.Value{
			Key:   key,
			Value: value,
		},
	}

	tree.rWLock.Lock()
	defer tree.rWLock.Unlock()

	if current == nil {
		tree.root = newNode
		tree.count++
		return kv.Value{}, false
	}

	for current != nil {
		// 如果已经存在键，则替换值
		if key == current.KV.Key {
			current.KV.Value = value
			isDeleted := current.KV.Deleted
			current.KV.Deleted = false
			// 返回旧值
			if isDeleted {
				return kv.Value{}, false
			} else {
				return current.KV, true
			}
		}
		// 要插入左边
		if key < current.KV.Key {
			// 左孩为空，直接插入左边
			if current.Left == nil {
				current.Left = newNode
				tree.count++
				return kv.Value{}, false
			}
			// 继续对比下一层
			current = current.Left
		} else {
			// 右孩为空，直接插入右边
			if current.Right == nil {
				current.Right = newNode
				tree.count++
				return kv.Value{}, false
			}
			// 继续对比下一层
			current = current.Right
		}
	}
	tree.count++
	return kv.Value{}, false
}

// Delete 删除并返回旧值，
// 返回的 bool 只表示是否有旧值
func (tree *SortTree) Delete(key string) (oldValue kv.Value, hasOld bool) {
	if tree == nil {
		log.Fatal("The tree is nil")
	}

	tree.rWLock.Lock()
	defer tree.rWLock.Unlock()
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
			// 存在且未被删除
			if current.KV.Deleted == false {
				current.KV.Deleted = true
				tree.count--
				return current.KV, true
			} else { // 已被删除过
				return kv.Value{}, false
			}
		}
		// 往下一层查找
		if key < current.KV.Key {
			// 如果不存在此 key，则插入一个删除标记
			if current.Left == nil {
				current.Left = newNode
			}
			// 继续对比下一层
			current = current.Left
		} else {
			// 如果不存在此 key，则插入一个删除标记
			if current.Right == nil {
				current.Right = newNode
			}
			// 继续对比下一层
			current = current.Right
		}
	}
	return kv.Value{}, false
}

// GetValues 获取树中的所有元素，这是一个有序元素列表
func (tree *SortTree) GetValues() []kv.Value {
	// 使用栈，而非递归
	stack := InitStack(tree.count / 2)
	values := make([]kv.Value, 0)

	tree.rWLock.RLock()
	defer tree.rWLock.RUnlock()

	// 使用栈非递归遍历树
	currentNode := tree.root
	for {
		if currentNode != nil {
			stack.Push(currentNode)
			currentNode = currentNode.Left
		} else {
			popNode, success := stack.Pop()
			if success == false {
				break
			}
			values = append(values, popNode.KV)
			currentNode = popNode.Right
		}
	}
	return values
}
