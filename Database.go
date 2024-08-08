package lsm

import (
	"github.com/huiming23344/lsm/ssTable"
	"github.com/huiming23344/lsm/wal"
	"log"
	"os"
	"path"
)

type Database struct {
	// 内存表
	MemTable *MemTable
	// 只读内存表
	iMemTable *ReadOnlyMemTables
	// SSTable 列表
	TableTree *ssTable.TableTree
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
			d.iMemTable.AddTable(table)
		}
	}
	return
}

func (d *Database) Swap() {
	table := d.MemTable.Swap()
	// 将内存表存储到 iMemTable 中
	log.Printf("add table to iMemTable, table: %v\n", table)
	d.iMemTable.AddTable(table)
}
