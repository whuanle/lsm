package sortTree

import (
	"github.com/whuanle/lsm/kv"
	"reflect"
	"testing"
)

func Test_SortTree_Insert(t *testing.T) {
	tree := &Tree{}
	tree.Init()
	_, hasOld := tree.Set("a", []byte{1, 2, 3})
	if hasOld == true {
		t.Error(hasOld)
	}

	oldKV, hasOld := tree.Set("a", []byte{2, 3, 4})
	if !hasOld {
		t.Error("fail to test the set function, the 'hasOld' should be true")
	}
	if !reflect.DeepEqual(oldKV.Value, []byte{1, 2, 3}) {
		t.Error("fail to test the set function, the 'oldKV' is invalid")
	}

	count := tree.GetCount()
	if count != 1 {
		t.Error(count)
	}

	tree.Set("b", []byte{1, 2, 3})
	tree.Set("c", []byte{1, 2, 3})

	count = tree.GetCount()
	if count != 3 {
		t.Error(count)
	}

	tree.Delete("a")
	tree.Delete("a")

	count = tree.GetCount()
	if count != 2 {
		t.Error(count)
	}

	data, success := tree.Search("a")
	if success != kv.Deleted {
		t.Error(success)
	}

	data, success = tree.Search("b")
	if success != kv.Success {
		t.Error(success)
	}

	if data.Value[0] != 1 {
		t.Error(data)
	}
}
