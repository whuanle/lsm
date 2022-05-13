package Memory

import (
	"testing"
)

func Test_SortTree_Insert(t *testing.T) {
	tree := &SortTree{}
	result := tree.Insert("a", []byte{1, 2, 3})
	if !result {
		t.Error(result)
	}

	count := tree.GetCount()
	if count != 1 {
		t.Error(count)
	}

	tree.Insert("b", []byte{1, 2, 3})
	tree.Insert("c", []byte{1, 2, 3})

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

	if data[0] != 1 {
		t.Error(data)
	}
}
