package sortTree

import (
	"testing"
)

func Test_SortTree_Insert(t *testing.T) {
	tree := &Tree{}
	_, hasOld := tree.Set("a", []byte{1, 2, 3})
	if hasOld == true {
		t.Error(hasOld)
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

	count = tree.GetCount()
	if count != 2 {
		t.Error(count)
	}

	data, success := tree.Search("a")
	if success {
		t.Error(success)
	}

	data, success = tree.Search("b")
	if !success {
		t.Error(success)
	}

	if data.Value[0] != 1 {
		t.Error(data)
	}
}
