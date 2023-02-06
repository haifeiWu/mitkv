package lsmtree

import (
	"testing"
)

func TestSkipList_Insert(t *testing.T) {
	type args struct {
		key   string
		value interface{}
	}

	tests := []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{
			name: "",
			args: args{
				key:   "key1",
				value: "value1",
			},
		},
	}
	s := NewSkipList()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s.Insert(tt.args.key, tt.args.value)
			val, exist := s.Search(tt.args.key)
			if !exist {
				t.Fatal("insert data err")
			}
			switch val.(type) {
			case string:
				convVal, ok := val.(string)
				if !ok {
					t.Fatal("insert data err")
				}
				if convVal != tt.args.value {
					t.Fatal("insert data err")
				}
			}
		})
	}
}
