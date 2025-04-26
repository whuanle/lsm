package skipList

import (
	"LSM/kv"
	"LSM/orderTable"
	"math/rand"
	"sync"
)

const MAXLEVEL = 16

type ListNode struct {
	KV   kv.Value
	next []*ListNode
}
type SkipList struct {
	head  *ListNode
	level int
	mutex *sync.RWMutex
	count int
}

func NewNode(value kv.Value, level int) *ListNode {
	return &ListNode{
		KV:   value,
		next: make([]*ListNode, level),
	}
}
func NewSkipList() *SkipList {
	return &SkipList{
		head:  NewNode(kv.Value{}, MAXLEVEL),
		level: 1,
	}
}
func (sl *SkipList) randomLevel() int {
	level := 1
	for rand.Float32() < 0.5 && level < MAXLEVEL {
		level++
	}
	return level
}
func (sl *SkipList) Init() {
	sl.head = NewNode(kv.Value{}, MAXLEVEL)
	sl.count = 0
	sl.mutex = &sync.RWMutex{}
	sl.level = 1
}
func (sl *SkipList) Set(key string, value []byte) (oldValue kv.Value, hasOld bool) {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()
	update := make([]*ListNode, MAXLEVEL)
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].KV.Key < key {
			current = current.next[i]
		}
		update[i] = current
	}
	if current.next[0] != nil && current.next[0].KV.Key == key {
		oldValue = current.next[0].KV
		hasOld = true
		current.next[0].KV.Value = value
		current.next[0].KV.Delete = false
		return
	}
	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}
	newNode := NewNode(kv.Value{Key: key, Value: value, Delete: false}, level)
	for i := 0; i < level; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}
	sl.count++
	return kv.Value{}, false

}
func (sl *SkipList) GetCount() int {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()
	return sl.count

}
func (sl *SkipList) Delete(key string) (oldValue kv.Value, hasOld bool) {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()
	update := make([]*ListNode, MAXLEVEL)
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.next[i] != nil && current.next[i].KV.Key < key {
			current = current.next[i]
		}
		update[i] = current
	}
	if current.next[0] != nil && current.next[0].KV.Key == key {
		current.next[0].KV.Delete = true
		return current.next[0].KV, true
	}
	level := sl.randomLevel()
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}
	newNode := NewNode(kv.Value{key, nil, true}, level)
	for i := sl.level; i < level; i++ {
		newNode.next[i] = update[i].next[i]
		update[i].next[i] = newNode
	}
	sl.count++
	return kv.Value{}, false

}
func (sl *SkipList) GetValues() []kv.Value {
	sl.mutex.RLock()
	defer sl.mutex.RUnlock()
	var values []kv.Value
	current := sl.head.next[0]
	for current != nil {
		values = append(values, current.KV)
		current = current.next[0]
	}
	return values

}
func (sl *SkipList) Search(key string) (kv.Value, kv.SearchResult) {
	sl.mutex.RLock()         // 加读锁保证并发安全
	defer sl.mutex.RUnlock() // 函数结束时释放锁

	// 从最高层开始搜索
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		// 在当前层向右搜索，直到找到大于或等于key的节点
		for current.next[i] != nil && current.next[i].KV.Key < key {
			current = current.next[i]
		}
	}

	// 移动到最底层的下一个节点（可能是目标节点）
	current = current.next[0]

	// 检查是否找到匹配的键
	if current != nil && current.KV.Key == key {
		// 找到匹配的键，返回值和成功状态
		return current.KV, kv.Success
	}

	// 未找到键，返回空值和未找到状态
	return kv.Value{}, kv.NotFind
}
func (sl *SkipList) Swap() orderTable.OrderInterface {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()
	newSl := NewSkipList()
	newSl.head, sl.head = sl.head, newSl.head
	newSl.level, sl.level = sl.level, newSl.level
	newSl.mutex = sl.mutex
	sl.mutex = &sync.RWMutex{}
	sl.count = 0
	return newSl
}
