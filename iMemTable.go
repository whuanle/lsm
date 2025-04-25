package LSM

import (
	"LSM/kv"
	"sync"
)

type ReadOnlyMemTables struct {
	readonlyTable []*MemTable
	lock          *sync.RWMutex
}

func (r *ReadOnlyMemTables) Init() {
	r.readonlyTable = make([]*MemTable, 0)
	r.lock = &sync.RWMutex{}
}
func (r *ReadOnlyMemTables) Getlen() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return len(r.readonlyTable)
}
func (r *ReadOnlyMemTables) AddTable(table *MemTable) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.readonlyTable = append(r.readonlyTable, table)
}
func (r *ReadOnlyMemTables) PopTable() *MemTable {
	r.lock.Lock()
	defer r.lock.Unlock()
	table := r.readonlyTable[0]
	r.readonlyTable = r.readonlyTable[1:]
	return table
}
func (r *ReadOnlyMemTables) Search(key string) (kv.Value, kv.SearchResult) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	for _, table := range r.readonlyTable {
		value, result := table.Search(key)
		if result == kv.Success {
			return value, result
		}
	}
	return kv.Value{}, kv.NotFind
}
