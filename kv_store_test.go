package mitkv

import (
	"strconv"
	"testing"
	"time"
)

const dataDir = "./db/"

func TestInitLSMKvStore(t *testing.T) {
	//dataDir := "/Users/bytedance/Work/openSource/db/"
	kvStore := InitLSMKvStore(dataDir, 4, 3)
	kvStore.Del("key_5")
	val := kvStore.Get("key_5")
	if val == "" {
		t.Log("PASS")
	}
	kvStore.Close()
}

func TestKvStoreSet(t *testing.T) {
	// TODO some bugs in the code
	kvStore := InitLSMKvStore(dataDir, 4, 3)
	for i := 0; i < 11; i++ {
		kvStore.Set("key_"+strconv.Itoa(i), "value_"+strconv.Itoa(i))
	}
	time.Sleep(time.Second * 2)
	kvStore.Close()
}

func TestKvStoreGet(t *testing.T) {
	kvStore := InitLSMKvStore(dataDir, 4, 3)
	for i := 0; i < 20; i++ {
		val := kvStore.Get("key_" + strconv.Itoa(i))
		t.Log("key", "key_"+strconv.Itoa(i), "val", val)
	}
	kvStore.Close()
}

func TestKvStoreDel(t *testing.T) {
	//kvStore := InitLSMKvStore(dataDir, 4, 3)
	//kvStore.Del("key_7")
	//kvStore.Del("key_6")
	//kvStore.Del("key_5")
	//kvStore.Del("key_1")
	//kvStore.Close()

	kvStore := InitLSMKvStore(dataDir, 4, 3)
	for i := 0; i < 10; i++ {
		val := kvStore.Get("key_" + strconv.Itoa(i))
		if val == "" || len(val) == 0 {
			t.Log("key", "key_"+strconv.Itoa(i), "val", "not exist")
			continue
		}
		t.Log("key", "key_"+strconv.Itoa(i), "val", val)
	}
	kvStore.Close()
}
