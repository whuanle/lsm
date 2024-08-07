package lsm

import (
	"github.com/huiming23344/lsm/config"
	"github.com/huiming23344/lsm/kv"
	"github.com/huiming23344/lsm/sortTree"
	"github.com/huiming23344/lsm/wal"
	"log"
	"sync"
)

type MemTable struct {
	// 内存表
	MemoryTree *sortTree.Tree
	// WalF 文件句柄
	Wal      *wal.Wal
	swapLock *sync.RWMutex
}

func (m *MemTable) InitMemTree() {
	log.Println("Initializing MemTable MemTree...")
	m.MemoryTree = &sortTree.Tree{}
	m.MemoryTree.Init()
	m.swapLock = &sync.RWMutex{}
}

func (m *MemTable) InitWal(dir string) {
	log.Println("Initializing MemTable Wal...")
	m.Wal = &wal.Wal{}
	m.Wal.Init(dir)
}

func (m *MemTable) Swap() *MemTable {
	con := config.GetConfig()
	m.swapLock.Lock()
	tmpTree := m.MemoryTree.Swap()
	// 创建一个新的只读内存表
	table := &MemTable{
		MemoryTree: tmpTree,
		Wal:        m.Wal,
	}
	// creat new wal
	newWal := &wal.Wal{}
	newWal.Init(con.DataDir)
	m.Wal = newWal
	m.swapLock.Unlock()
	return table
}

func (m *MemTable) Search(key string) (kv.Value, kv.SearchResult) {
	m.swapLock.RLock()
	defer m.swapLock.RUnlock()
	return m.MemoryTree.Search(key)
}

func (m *MemTable) Set(key string, value []byte) (kv.Value, bool) {
	m.swapLock.RLock()
	defer m.swapLock.RUnlock()
	oldValue, hasOld := m.MemoryTree.Set(key, value)
	m.Wal.Write(kv.Value{
		Key:     key,
		Value:   value,
		Deleted: false,
	})
	return oldValue, hasOld
}

func (m *MemTable) Delete(key string) (kv.Value, bool) {
	m.swapLock.RLock()
	defer m.swapLock.RUnlock()
	oldValue, success := m.MemoryTree.Delete(key)
	if success {
		m.Wal.Write(kv.Value{
			Key:     key,
			Value:   nil,
			Deleted: true,
		})
	}
	return oldValue, success
}
