package sstable

import "encoding/json"

// KVData 一个数据
type KVData struct {
	Key     string `json:"key"`
	deleted bool   `json:"deleted"`
	Value   []byte `json:"value"`
}

func (e *KVData) Encode() ([]byte, error) {
	bytes, err := json.Marshal(e)
	if err != nil {
		return []byte{}, err
	}
	return bytes, nil
}

func Decode(bytes []byte) (KVData, error) {
	var e KVData
	err := json.Unmarshal(bytes, &e)
	return e, err
}
