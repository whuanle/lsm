package kv

import (
	"reflect"
	"testing"
)

type vTest struct {
	A int
	B int
}

var testData []byte = []byte{123, 34, 65, 34, 58, 49, 50, 51, 44, 34, 66, 34, 58, 49, 50, 51, 125}

func Test_Value_Convert(t *testing.T) {
	data, err := Convert(vTest{
		A: 123,
		B: 123,
	})
	if err != nil {
		t.Error(err)
	}
	if reflect.DeepEqual(data, testData) == false {
		t.Fatal()
	}
}

func Test_Value_Get(t *testing.T) {

}

func Test_Value_Encode(t *testing.T) {

}

func Test_Value_Decode(t *testing.T) {

}
