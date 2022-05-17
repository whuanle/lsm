package lsm

import (
	"encoding/json"
	"github.com/whuanle/lsm/kv"
	"log"
)

// Get 获取一个元素
func Get[T any](key string) (T, bool) {
	log.Print("Get ", key)
	// 先查内存表
	value, result := database.MemoryTree.Search(key)

	if result == kv.Success {
		return getInstance[T](value.Value)
	}

	// 查 SsTable 文件
	if database.TableTree != nil {
		value, result := database.TableTree.Search(key)
		if result == kv.Success {
			return getInstance[T](value.Value)
		}
	}
	var nilV T
	return nilV, false
}

// Set 插入元素
func Set[T any](key string, value T) bool {
	log.Print("Insert ", key, ",")
	data, err := kv.Convert(value)
	if err != nil {
		log.Println(err)
		return false
	}

	_, _ = database.MemoryTree.Set(key, data)

	// 写入 wal.log
	database.Wal.Write(kv.Value{
		Key:     key,
		Value:   data,
		Deleted: false,
	})
	return true
}

// DeleteAndGet 删除元素并尝试获取旧的值，
// 返回的 bool 表示是否有旧值，不表示是否删除成功
func DeleteAndGet[T any](key string) (T, bool) {
	log.Print("Delete ", key)
	value, success := database.MemoryTree.Delete(key)

	if success {
		// 写入 wal.log
		database.Wal.Write(kv.Value{
			Key:     key,
			Value:   nil,
			Deleted: true,
		})
		return getInstance[T](value.Value)
	}
	var nilV T
	return nilV, false
}

// Delete 删除元素
func Delete[T any](key string) {
	log.Print("Delete ", key)
	database.MemoryTree.Delete(key)
	database.Wal.Write(kv.Value{
		Key:     key,
		Value:   nil,
		Deleted: true,
	})
}

// 将字节数组转为类型对象
func getInstance[T any](data []byte) (T, bool) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		log.Println(err)
	}
	return value, true
}
