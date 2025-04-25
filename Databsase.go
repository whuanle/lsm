package LSM

import (
	"LSM/ssTable"
	"LSM/wal"
	"log"
	"os"
	"path"
)

type Database struct {
	MemTable  *MemTable
	iMemTable *ReadOnlyMemTables
	TableTree *ssTable.TableTree
}

var database *Database

func (d *Database) loadAllWalFiles(dir string) {
	infos, err := os.ReadDir(dir)
	if err != nil {
		log.Println("Failed to read the database file")
		panic(err)
	}
	orderTable := d.MemTable.OrderTable
	for _, info := range infos {
		name := info.Name()
		if path.Ext(name) == ".log" {
			preWal := &wal.Wal{}
			preTable := preWal.LoadFromFile(path.Join(dir, info.Name()), orderTable)
			table := &MemTable{
				OrderTable: preTable,
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
	log.Printf("add table to iMemTable, table: %v\n", table)
	d.iMemTable.AddTable(table)
}
