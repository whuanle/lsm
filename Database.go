package lsm

import (
	"github.com/whuanle/lsm/sortTree"
	"github.com/whuanle/lsm/ssTable"
	"github.com/whuanle/lsm/wal"
	"sync"
)

type Database struct {
	// 内存表
	MemoryTree *sortTree.Tree
	// SsTable 列表
	TableTree *ssTable.TableTree
	// WalF 文件句柄
	Wal *wal.Wal
	// 内存锁，当内存表要同步到磁盘文件时，使用写锁，其余 MemoryTree 的操作均是读锁
	MemoryLock *sync.RWMutex
}

// 数据库，全局唯一实例
var database *Database
