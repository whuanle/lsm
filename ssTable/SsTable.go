package ssTable

import (
	"os"
	"sync"
)

// SSTable 表，存储在磁盘文件中
type SSTable struct {
	// 文件句柄，要注意，操作系统的文件句柄是有限的
	f        *os.File
	filePath string
	// 元数据
	tableMetaInfo MetaInfo
	// 文件的稀疏索引列表
	sparseIndex map[string]Position
	// 排序后的 key 列表
	sortIndex []string
	// SSTable 只能使排他锁
	lock sync.Locker
	/*
		sortIndex 是有序的，便于 CPU 缓存等，还可以使用布隆过滤器，有助于快速查找。
		sortIndex 找到后，使用 sparseIndex 快速定位
	*/
}

// TableTree 树
type TableTree struct {
	levels []*tableNode
	// 用于避免进行插入或压缩、删除 SSTable 时发生冲突
	lock *sync.RWMutex
}

// 链表，表示每一层的 SSTable
type tableNode struct {
	index int
	table *SSTable
	next  *tableNode
}

func (table *SSTable) Init(path string) {
	table.filePath = path
	table.loadFileHandle()
}
