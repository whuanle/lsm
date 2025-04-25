package ssTable

import (
	"LSM/kv"
	"log"
	"os"
	"sync"
)

type SSTable struct {
	f        *os.File
	filePath string
	//元数据
	tableMetaInfo MetaInfo
	//文件稀疏索引
	sparseIndex map[string]Position
	//排序后的key列表
	sortIndex []string
	lock      sync.Locker
}

func (table *SSTable) Init(filePath string) {
	table.filePath = filePath
	table.lock = &sync.Mutex{}
	table.loadFileHandle()
}
func (table *SSTable) Search(key string) (value kv.Value, result kv.SearchResult) {
	table.lock.Lock()
	defer table.lock.Unlock()
	var position = Position{
		Start: -1,
	}
	l := 0
	r := len(table.sortIndex) - 1
	for l <= r {
		mid := (l + r) / 2
		if table.sortIndex[mid] == key {
			position = table.sparseIndex[key]
			if position.Deleted {
				return kv.Value{}, kv.Delete
			}
			break
		} else if table.sortIndex[mid] < key {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	if position.Start == -1 {
		return kv.Value{}, kv.NotFind
	}
	bytes := make([]byte, position.Len)
	if _, err := table.f.Seek(position.Start, 0); err != nil {
		log.Println(err)
		return kv.Value{}, kv.NotFind
	}
	if _, err := table.f.Read(bytes); err != nil {
		log.Println(err)
		return kv.Value{}, kv.NotFind
	}
	value, err := kv.Decode(bytes)
	if err != nil {
		log.Println(err)
		return kv.Value{}, kv.NotFind
	}
	return value, kv.Success
}
