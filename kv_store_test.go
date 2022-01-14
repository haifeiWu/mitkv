package mitkv

import (
	"strconv"
	"testing"
)

func TestInitLSMKvStore(t *testing.T) {
	dataDir := "/Users/bytedance/Work/openSource/db/"
	kvStore := InitLSMKvStore(dataDir, 4, 3)
	kvStore.close()
}

func TestKvStoreSet(t *testing.T) {
	// TODO some bugs in the code
	dataDir := "/Users/bytedance/Work/openSource/db/"
	kvStore := InitLSMKvStore(dataDir, 4, 3)
	for i := 0; i < 11; i++ {
		kvStore.Set("key_"+strconv.Itoa(i), "value_"+strconv.Itoa(i))
	}
	kvStore.close()
}

func TestKvStoreGet(t *testing.T) {
	dataDir := "/Users/bytedance/Work/openSource/db/"
	kvStore := InitLSMKvStore(dataDir, 4, 3)
	for i := 0; i < 10; i++ {
		val := kvStore.Get("key_" + strconv.Itoa(i))
		t.Log("key", "key_"+strconv.Itoa(i), "val", val)
	}
	kvStore.close()
}

func TestKvStoreDel(t *testing.T) {
	dataDir := "/Users/bytedance/Work/openSource/db/"
	kvStore := InitLSMKvStore(dataDir, 4, 3)
	kvStore.Del("key_2")
	kvStore.Del("key_1")
	kvStore.close()

	kvStore = InitLSMKvStore(dataDir, 4, 3)
	for i := 0; i < 10; i++ {
		val := kvStore.Get("key_" + strconv.Itoa(i))
		if val == "" || len(val) == 0 {
			t.Log("key", "key_"+strconv.Itoa(i), "val", "not exist")
			continue
		}
		t.Log("key", "key_"+strconv.Itoa(i), "val", val)
	}
	kvStore.close()
}
