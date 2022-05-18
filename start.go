package lsm

import (
	"github.com/whuanle/lsm/config"
	"github.com/whuanle/lsm/sortTree"
	"github.com/whuanle/lsm/ssTable"
	"github.com/whuanle/lsm/wal"
	"log"
	"os"
)

// Start 启动数据库
func Start(con config.Config) {
	if database != nil {
		return
	}
	// 将配置保存到内存中
	log.Println("Loading a Configuration File")
	config.Init(con)
	// 初始化数据库
	log.Println("Initializing the database")
	initDatabase(con.DataDir)

	// 数据库启动前进行一次数据压缩
	log.Println("Performing background checks...")
	// 检查内存
	checkMemory()
	// 检查压缩数据库文件
	database.TableTree.Check()
	// 启动后台线程
	go Check()
}

// 初始化 Database，从磁盘文件中还原 SSTable、WalF、内存表等
func initDatabase(dir string) {
	database = &Database{
		MemoryTree: &sortTree.Tree{},
		Wal:        &wal.Wal{},
		TableTree:  &ssTable.TableTree{},
	}
	// 从磁盘文件中恢复数据
	// 如果目录不存在，则为空数据库
	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist. The directory is being created\r\n", dir)
		err := os.Mkdir(dir, 0666)
		if err != nil {
			log.Println("Failed to create the database directory")
			panic(err)
		}
	}
	// 从数据目录中，加载 WalF、database 文件
	// 非空数据库，则开始恢复数据，加载 WalF 和 SSTable 文件
	memoryTree := database.Wal.Init(dir)

	database.MemoryTree = memoryTree
	log.Println("Loading database...")
	database.TableTree.Init(dir)
}
