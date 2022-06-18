package sortTree

import (
	"github.com/whuanle/lsm/kv"
	"log"
	"sync"
)

// treeNode 有序树节点
type treeNode struct {
	KV    kv.Value
	Left  *treeNode
	Right *treeNode
}

// Tree 有序树
type Tree struct {
	root   *treeNode
	count  int
	rWLock *sync.RWMutex
}

// Init 初始化树
func (tree *Tree) Init() {
	tree.rWLock = &sync.RWMutex{}
}

// GetCount 获取树中的元素数量
func (tree *Tree) GetCount() int {
	return tree.count
}

// Search 查找 Key 的值
func (tree *Tree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.rWLock.RLock()
	defer tree.rWLock.RUnlock()

	if tree == nil {
		log.Fatal("The tree is nil")
	}

	currentNode := tree.root
	// 有序查找
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

// Set 设置 Key 的值并返回旧值
func (tree *Tree) Set(key string, value []byte) (oldValue kv.Value, hasOld bool) {
	tree.rWLock.Lock()
	defer tree.rWLock.Unlock()

	if tree == nil {
		log.Fatal("The tree is nil")
	}

	current := tree.root
	newNode := &treeNode{
		KV: kv.Value{
			Key:   key,
			Value: value,
		},
	}

	if current == nil {
		tree.root = newNode
		tree.count++
		return kv.Value{}, false
	}

	for current != nil {
		// 如果已经存在键，则替换值
		if key == current.KV.Key {
			oldKV := current.KV.Copy()
			current.KV.Value = value
			current.KV.Deleted = false
			// 返回旧值
			if oldKV.Deleted {
				return kv.Value{}, false
			} else {
				return *oldKV, true
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
	log.Fatalf("The tree fail to Set value, key: %s, value: %v", key, value)
	return kv.Value{}, false
}

// Delete 删除 key 并返回旧值
func (tree *Tree) Delete(key string) (oldValue kv.Value, hasOld bool) {
	tree.rWLock.Lock()
	defer tree.rWLock.Unlock()

	if tree == nil {
		log.Fatal("The tree is nil")
	}

	newNode := &treeNode{
		KV: kv.Value{
			Key:     key,
			Value:   nil,
			Deleted: true,
		},
	}

	currentNode := tree.root
	if currentNode == nil {
		tree.root = newNode
		return kv.Value{}, false
	}

	for currentNode != nil {
		if key == currentNode.KV.Key {
			// 存在且未被删除
			if currentNode.KV.Deleted == false {
				oldKV := currentNode.KV.Copy()
				currentNode.KV.Value = nil
				currentNode.KV.Deleted = true
				// count 应该是统计当前树中存在的有效节点，但是如果删除一个不存在的key，这个count会计算错误
				// 应该要在添加删除Node的时候count增加一下来保证count数量正确
				tree.count--
				return *oldKV, true
			} else { // 已被删除过
				return kv.Value{}, false
			}
		}
		// 往下一层查找
		if key < currentNode.KV.Key {
			// 如果不存在此 key，则插入一个删除标记
			if currentNode.Left == nil {
				currentNode.Left = newNode
				tree.count++
			}
			// 继续对比下一层
			currentNode = currentNode.Left
		} else {
			// 如果不存在此 key，则插入一个删除标记
			if currentNode.Right == nil {
				currentNode.Right = newNode
				tree.count++
			}
			// 继续对比下一层
			currentNode = currentNode.Right
		}
	}
	log.Fatalf("The tree fail to delete key, key: %s", key)
	return kv.Value{}, false
}

// GetValues 获取树中的所有元素，这是一个有序元素列表
func (tree *Tree) GetValues() []kv.Value {
	tree.rWLock.RLock()
	defer tree.rWLock.RUnlock()

	// 使用栈，而非递归，栈使用了切片，可以自动扩展大小，不必担心栈满
	stack := InitStack(tree.count / 2)
	values := make([]kv.Value, 0)

	// 从小到大获取树的元素
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

func (tree *Tree) Swap() *Tree {
	tree.rWLock.Lock()
	defer tree.rWLock.Unlock()

	newTree := &Tree{}
	newTree.Init()
	newTree.root = tree.root
	tree.root = nil
	tree.count = 0
	return newTree
}
