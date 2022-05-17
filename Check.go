package lsm

import (
	"github.com/whuanle/lsm/config"
	"github.com/whuanle/lsm/sortTree"
	"log"
	"time"
)

func Check() {
	for {
		time.Sleep(10 * time.Second)
		log.Println("Performing background checks...")
		// 检查内存
		checkMemory()
		// 检查压缩数据库文件
		database.TableTree.Check()
	}
}

func checkMemory() {
	con := config.GetConfig()
	if database.MemoryTree.GetCount() < con.Threshold {
		return
	}
	database.MemoryLock.Lock()
	tmpTree := database.MemoryTree
	database.MemoryTree = &sortTree.Tree{}
	database.MemoryLock.Unlock()
	// 将内存表存储到 SsTable 中
	database.TableTree.CreateNewTable(tmpTree.GetValues())
}
