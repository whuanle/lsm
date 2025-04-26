package LSM

import (
	"LSM/config"
	"LSM/kv"
	"LSM/orderTable"
	"LSM/orderTable/skipList"
	"LSM/wal"
	"log"
	"sync"
)

type MemTable struct {
	OrderTable orderTable.OrderInterface
	Wal        *wal.Wal
	swapLock   *sync.RWMutex
}

func (m *MemTable) InitMemTree() {
	log.Println("Initializing MemTable MemTree...")
	m.OrderTable = &skipList.SkipList{}
	m.OrderTable.Init()
	m.swapLock = &sync.RWMutex{}
}
func (m *MemTable) InitWal(dir string) {
	log.Println("Initializing Wal...")
	m.Wal = &wal.Wal{}
	m.Wal.Init(dir)
}
func (m *MemTable) Swap() *MemTable {
	con := config.GetConfig()
	m.swapLock.Lock()
	defer m.swapLock.Unlock()
	tmpTable := m.OrderTable.Swap()
	table := &MemTable{
		OrderTable: tmpTable,
		Wal:        m.Wal,
		swapLock:   &sync.RWMutex{},
	}
	newWal := &wal.Wal{}
	newWal.Init(con.DataDir)
	m.Wal = newWal
	return table
}
func (m *MemTable) Search(key string) (kv.Value, kv.SearchResult) {
	m.swapLock.RLock()
	defer m.swapLock.RUnlock()
	return m.OrderTable.Search(key)
}
func (m *MemTable) Set(key string, value []byte) (kv.Value, bool) {
	m.swapLock.Lock()
	defer m.swapLock.Unlock()
	oldValue, hasOld := m.OrderTable.Set(key, value)
	m.Wal.Write(kv.Value{
		Key:    key,
		Value:  value,
		Delete: false,
	})
	return oldValue, hasOld
}
func (m *MemTable) Delete(key string) (kv.Value, bool) {
	m.swapLock.Lock()
	defer m.swapLock.Unlock()
	oldValue, success := m.OrderTable.Delete(key)
	if success {
		m.Wal.Write(kv.Value{
			Key:    key,
			Value:  nil,
			Delete: true,
		})
	}
	return oldValue, success
}
