package mitkv

import (
	"errors"
	"strconv"
	"testing"
)

const dataDir = "./db/"

func TestInitLSMKvStore(t *testing.T) {
	//dataDir := "/Users/bytedance/Work/openSource/db/"
	kvStore, err := InitLSMKvStore(dataDir, 4, 3)
	if err != nil {
		t.Fatal(err)
	}
	kvStore.Del("key_5")
	val := kvStore.Get("key_5")
	if val == "" {
		t.Log("PASS")
	}
	kvStore.Close()
}

func TestLSMKvStore_Get(t *testing.T) {
	type args struct {
		key string
	}
	var tests = []struct {
		name      string
		args      args
		wantValue string
	}{
		{
			name: "get key_1",
			args: args{
				key: "key_1",
			},
			wantValue: "val_1",
		},
		{
			name: "get key_2",
			args: args{
				key: "key_2",
			},
			wantValue: "val_2",
		},
	}

	kv, err := InitLSMKvStore(dataDir, 4, 3)
	if err != nil {
		t.Fatal(err)
	}

	for _, tt := range tests {
		kv.Set(tt.args.key, tt.wantValue)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotValue := kv.Get(tt.args.key); gotValue != tt.wantValue {
				t.Errorf("Get() = %v, want %v", gotValue, tt.wantValue)
			}
		})
	}

	for _, tt := range tests {
		kv.Del(tt.args.key)
	}
}

func TestLSMKvStore_Set(t *testing.T) {
	type args struct {
		key   string
		value string
	}

	kv, err := InitLSMKvStore(dataDir, 4, 3)
	if err != nil {
		t.Fatal(err)
	}

	var tests = []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{name: "key_1", args: args{
			key:   "key_1",
			value: "val_1",
		}},
		{name: "key_2", args: args{
			key:   "key_2",
			value: "val_1",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv.Set(tt.args.key, tt.args.value)
			val := kv.Get(tt.args.key)
			if val != tt.args.value {
				t.Fatal(errors.New("set val not equal to get val"))
			}
			// clear data
			kv.Del(tt.args.key)
		})
	}
}

func TestLSMKvStore_Del(t *testing.T) {
	type args struct {
		key string
	}

	kv, err := InitLSMKvStore(dataDir, 4, 3)
	if err != nil {
		t.Fatal(err)
	}

	var tests = []struct {
		name string
		args args
	}{
		// TODO: Add test cases.
		{name: "key_1", args: args{
			key: "key_1",
		}},
		{name: "key_2", args: args{
			key: "key_2",
		}},
	}

	for i := 0; i < 2; i++ {
		kv.Set("key_"+strconv.Itoa(i), "val_"+strconv.Itoa(i))
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kv.Del(tt.args.key)
		})
	}
}
