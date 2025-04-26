package LSM

import (
	"LSM/config"
	"log"
	"time"
)

func Check() {
	//con := config.GetConfig()
	ticker := time.Tick(50 * time.Millisecond)
	for range ticker {
		log.Println("Performing background checks...")
		// 检查内存
		checkMemory()
		// 检查压缩数据库文件
		database.TableTree.Check()
	}
}

func checkMemory() {
	con := config.GetConfig()
	count := database.MemTable.OrderTable.GetCount()
	if count < con.Threshold {
		return
	}
	// 交互内存
	log.Println("Compressing memory")
	database.Swap()
}

// CompressMemory 会监听iMemTable，当iMemTable有数据的时候就进行压缩
func CompressMemory() {
	con := config.GetConfig()
	ticker := time.Tick(time.Duration(con.CompressInterval) * time.Millisecond)
	for range ticker {
		//fmt.Println(database.iMemTable.Getlen())
		for database.iMemTable.Getlen() != 0 {
			log.Println("Compressing iMemTable")
			preTable := database.iMemTable.PopTable()
			database.TableTree.CreatNewTable(preTable.OrderTable.GetValues())
			preTable.Wal.DeleteFile()
		}
	}
}
