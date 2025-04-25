package orderTable

import "LSM/kv"

type OrderInterface interface {
	Set(key string, value []byte) (oldValue kv.Value, hasOld bool)
	GetCount() int
	Delete(key string) (oldValue kv.Value, hasOld bool)
	GetValues() []kv.Value
	Init()
	//Swap() *OrderInterface
	Swap() OrderInterface
	Search(key string) (kv.Value, kv.SearchResult)
}
