package kv

import "encoding/json"

// SearchResult 查找结果
type SearchResult int

const (
	// None 没有查找到
	None SearchResult = iota
	// Deleted 已经被删除
	Deleted
	// Success 查找成功
	Success
)

// Value 表示一个 KV
type Value struct {
	Key     string
	Value   []byte
	Deleted bool
}

func (v *Value) Copy() *Value {
	return &Value{
		Key:     v.Key,
		Value:   v.Value,
		Deleted: v.Deleted,
	}
}

// Get 反序列化元素中的值
func Get[T any](v *Value) (T, error) {
	var value T
	err := json.Unmarshal(v.Value, &value)
	return value, err
}

// Convert 将值序列化为二进制
func Convert[T any](value T) ([]byte, error) {
	return json.Marshal(value)
}

// Decode 二进制数据反序列化为 Value
func Decode(data []byte) (Value, error) {
	var value Value
	err := json.Unmarshal(data, &value)
	return value, err
}

// Encode 将 Value 序列化为二进制
func Encode(value Value) ([]byte, error) {
	return json.Marshal(value)
}
