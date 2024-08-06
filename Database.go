package lsm

import (
	"github.com/whuanle/lsm/ssTable"
	"github.com/whuanle/lsm/wal"
	"log"
	"os"
	"path"
	"sync"
)

type Database struct {
	// 内存表
	MemTable *MemTable
	// 只读内存表
	iMemTable *ReadOnlyMemTables
	// SSTable 列表
	TableTree *ssTable.TableTree
}

type ReadOnlyMemTables struct {
	// 只读内存表
	readonlyTable []*MemTable
	lock          sync.Locker
}

func (r *ReadOnlyMemTables) Init() {
	r.readonlyTable = make([]*MemTable, 0)
	r.lock = &sync.Mutex{}
}

func (r *ReadOnlyMemTables) Getlen() int {
	r.lock.Lock()
	r.lock.Unlock()
	return len(r.readonlyTable)
}

// 数据库，全局唯一实例
var database *Database

func (d *Database) loadAllWalFiles(dir string) {
	infos, err := os.ReadDir(dir)
	if err != nil {
		log.Println("Failed to read the database file")
		panic(err)
	}
	tree := d.MemTable.MemoryTree
	for _, info := range infos {
		// 如果是 wal.log 文件
		name := info.Name()
		if path.Ext(name) == ".log" {
			preWal := &wal.Wal{}
			preTree := preWal.LoadFromFile(path.Join(dir, info.Name()), tree)
			table := &MemTable{
				MemoryTree: preTree,
				Wal:        preWal,
			}
			log.Printf("add table to iMemTable, table: %v\n", table)
			d.iMemTable.Add(table)
		}
	}
	return
}

func (r *ReadOnlyMemTables) Add(table *MemTable) {
	r.lock.Lock()
	r.readonlyTable = append(r.readonlyTable, table)
	r.lock.Unlock()
}

func (r *ReadOnlyMemTables) Get() *MemTable {
	r.lock.Lock()
	defer r.lock.Unlock()
	table := r.readonlyTable[0]
	r.readonlyTable = r.readonlyTable[1:]
	return table
}

func (d *Database) Swap() {
	table := d.MemTable.Swap()
	// 将内存表存储到 iMemTable 中
	log.Printf("add table to iMemTable, table: %v\n", table)
	d.iMemTable.Add(table)
}
