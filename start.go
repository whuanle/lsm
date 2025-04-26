package LSM

import (
	"LSM/config"
	"LSM/ssTable"
	"log"
	"os"
)

func Start(con config.Config) {
	if database != nil {
		return
	}
	log.Println("Loading a Configuration File")
	config.Init(con)
	log.Println("Starting Server")
	initDatabase(con.DataDir)
	log.Println("Performing background checks...")
	checkMemory()
	// 检查压缩数据库文件
	database.TableTree.Check()
	go Check()
	go CompressMemory()
}
func initDatabase(dir string) {
	database = &Database{
		MemTable:  &MemTable{},
		iMemTable: &ReadOnlyMemTables{},
		TableTree: &ssTable.TableTree{},
	}
	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist. The directory is being created\r\n", dir)
		err := os.MkdirAll(dir, 0700)
		if err != nil {
			log.Println("Failed to create the database directory")
			panic(err)
		}
	}
	database.iMemTable.Init()
	database.MemTable.InitMemTree()
	log.Println("Loading all wal.log...")
	database.loadAllWalFiles(dir)
	database.MemTable.InitWal(dir)
	log.Println("Loading database...")
	database.TableTree.Init(dir)
}
