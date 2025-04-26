package LSM

import (
	"LSM/kv"
	"encoding/json"
	"log"
)

func Get[T any](key string) (T, bool) {
	log.Print("Get ", key)
	value, result := database.MemTable.Search(key)
	if result == kv.Success {
		return getInstance[T](value.Value)
	}
	value, result = database.iMemTable.Search(key)
	if result == kv.Success {
		return getInstance[T](value.Value)
	}
	if database.TableTree != nil {
		value, result = database.TableTree.Search(key)
		if result == kv.Success {
			return getInstance[T](value.Value)
		}
	}
	var nilV T
	return nilV, false
}
func Set[T any](key string, value T) bool {
	//log.Print("Set ", key, value)
	data, err := kv.Convert(value)
	if err != nil {
		log.Println(err)
		return false
	}
	_, _ = database.MemTable.Set(key, data)
	return true
}

// DeleteAndGet 删除元素并尝试获取旧的值，
// 返回的 bool 表示是否有旧值，不表示是否删除成功
func DeleteAndGet[T any](key string) (T, bool) {
	log.Print("Delete ", key)
	value, success := database.MemTable.Delete(key)
	if success {
		return getInstance[T](value.Value)
	}
	var nilV T
	return nilV, false
}
func Delete[T any](key string) {
	log.Print("Delete ", key)
	database.MemTable.Delete(key)
}
func getInstance[T any](data []byte) (T, bool) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		log.Println(err)
	}
	return value, true
}
