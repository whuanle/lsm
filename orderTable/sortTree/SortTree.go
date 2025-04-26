package sortTree

import (
	"LSM/kv"
	"LSM/orderTable"
	"log"
	"sync"
)

const (
	NotFound = "KeyNotFound"
)

type treeNode struct {
	KV    kv.Value
	Left  *treeNode
	Right *treeNode
}
type Tree struct {
	root   *treeNode
	count  int
	RWLock *sync.RWMutex
}

func (tree *Tree) Init() {
	tree.RWLock = &sync.RWMutex{}
}
func (tree *Tree) Count() int {
	return tree.count
}
func (tree *Tree) Search(key string) (kv.Value, kv.SearchResult) {
	tree.RWLock.RLock()
	defer tree.RWLock.RUnlock()

	if tree.root == nil {
		return kv.Value{}, kv.NotFind
		log.Fatal("The tree is nil")
	}
	currentNode := tree.root
	for currentNode != nil {
		if key == currentNode.KV.Key {
			if currentNode.KV.Delete == false {
				return currentNode.KV, kv.Success
			}
			return kv.Value{}, kv.Delete
		}
		if key < currentNode.KV.Key {
			currentNode = currentNode.Left
		} else {
			currentNode = currentNode.Right
		}
	}
	return kv.Value{}, kv.NotFind
}
func (tree *Tree) GetCount() int {
	return tree.count
}

// Set 设置 Key 的值并返回旧值
func (tree *Tree) Set(key string, value []byte) (oldValue kv.Value, hasOld bool) {
	tree.RWLock.Lock()
	defer tree.RWLock.Unlock()
	currentNode := tree.root
	newNode := &treeNode{
		KV: kv.Value{
			Key:   key,
			Value: value,
		},
	}
	if currentNode == nil {
		tree.root = newNode
		tree.count++
		return kv.Value{}, false
	}
	for currentNode != nil {
		if key == currentNode.KV.Key {
			oldKV := currentNode.KV.Copy()
			currentNode.KV.Value = value
			currentNode.KV.Delete = false
			if oldKV.Delete {
				return kv.Value{}, false
			}
			return *oldKV, true
		}
		if key < currentNode.KV.Key {
			if currentNode.Left == nil {
				currentNode.Left = newNode
				tree.count++
				return kv.Value{}, false
			}
			currentNode = currentNode.Left
		} else {
			if currentNode.Right == nil {
				currentNode.Right = newNode
				tree.count++
				return kv.Value{}, false
			}
			currentNode = currentNode.Right
		}
	}
	return kv.Value{}, false
}
func (tree *Tree) Delete(key string) (oldValue kv.Value, hasOld bool) {
	tree.RWLock.Lock()
	defer tree.RWLock.Unlock()
	currentNode := tree.root
	if currentNode == nil {
		return kv.Value{}, false
	}
	for currentNode != nil {
		if key == currentNode.KV.Key {
			if currentNode.KV.Delete {
				return kv.Value{}, false
			}
			oldKV := currentNode.KV.Copy()
			currentNode.KV.Value = nil
			currentNode.KV.Delete = true
			tree.count--
			return *oldKV, true
		}
		if key < currentNode.KV.Key {
			currentNode = currentNode.Left
		} else {
			currentNode = currentNode.Right
		}
	}
	return kv.Value{}, false
}
func (tree *Tree) GetValues() []kv.Value {
	tree.RWLock.RLock()
	defer tree.RWLock.RUnlock()
	stack := InitStack(tree.count / 2)
	values := make([]kv.Value, 0)
	currentNode := tree.root
	for {
		if currentNode != nil {
			stack.Push(currentNode)
			currentNode = currentNode.Left
		} else {
			popNode, success := stack.Pop()
			if !success {
				break
			}
			values = append(values, popNode.KV)
			currentNode = popNode.Right
		}
	}
	return values
}
func (tree *Tree) Swap() orderTable.OrderInterface {
	tree.RWLock.Lock()
	defer tree.RWLock.Unlock()
	newTree := &Tree{}
	newTree.Init()
	newTree.root = tree.root
	tree.root = nil
	tree.count = 0
	return newTree
}
