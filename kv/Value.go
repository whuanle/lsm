package KV

// Value 一个元素
type Value struct {
	Key     string `json:"key"`
	Value   []byte `json:"value"`
	Deleted bool   `json:"deleted"`
}
