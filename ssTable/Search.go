package ssTable

import (
	"github.com/whuanle/lsm/kv"
	"log"
)

// Search 查找元素，
// 先使用二分查找法从内存中的 keys 列表查找 Key，如果存在，找到 Position ，再通过从数据区加载
func (table *SSTable) Search(key string) (value kv.Value, result kv.SearchResult) {
	table.lock.Lock()
	defer table.lock.Unlock()

	// 元素定位
	var position = Position{
		Start: -1,
	}
	l := 0
	r := len(table.sortIndex) - 1

	// 二分查找法，查找 key 是否存在
	for l <= r {
		mid := (l + r) / 2
		if table.sortIndex[mid] == key {
			// 获取元素定位
			position = table.sparseIndex[key]
			// 如果元素已被删除，则返回
			if position.Deleted {
				return kv.Value{}, kv.Deleted
			}
			break
		} else if table.sortIndex[mid] < key {
			l = mid + 1
		} else if table.sortIndex[mid] > key {
			r = mid - 1
		}
	}

	if position.Start == -1 {
		return kv.Value{}, kv.None
	}

	// Todo：如果读取失败，需要增加错误处理过程
	// 从磁盘文件中查找
	bytes := make([]byte, position.Len)
	if _, err := table.f.Seek(position.Start, 0); err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	if _, err := table.f.Read(bytes); err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}

	value, err := kv.Decode(bytes)
	if err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	return value, kv.Success
}
