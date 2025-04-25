package kv

import (
	"encoding/json"
)

type SearchResult int

const (
	NotFind = iota
	Delete
	Success
)

type Value struct {
	Key    string
	Value  []byte
	Delete bool
}

func (v *Value) Copy() *Value {
	return &Value{
		Key:    v.Key,
		Value:  v.Value,
		Delete: v.Delete,
	}
}
func Get[T any](v *Value) (T, error) {
	var value T
	err := json.Unmarshal(v.Value, &value)
	return value, err
}
func Convert[T any](value T) ([]byte, error) {
	return json.Marshal(&value)
}
func Decode(data []byte) (Value, error) {
	var value Value
	err := json.Unmarshal(data, &value)
	return value, err
}
func Encode(value Value) ([]byte, error) {
	return json.Marshal(&value)
}
