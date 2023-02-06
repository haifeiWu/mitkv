package lsmtree

import (
	"testing"
)

func TestLSMTree_Insert(t *testing.T) {
	l := NewLSMTree()
	l.Insert("key1", []byte("value1"))
	val := l.Find("key1")
	if val != nil {
		t.Log(string(val))
	}
}
