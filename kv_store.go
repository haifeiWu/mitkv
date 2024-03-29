package mitkv

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	sll "github.com/emirpasic/gods/lists/singlylinkedlist"
	"github.com/emirpasic/gods/maps/treemap"
	avl "github.com/emirpasic/gods/trees/avltree"
)

// KvStore KvStore
type KvStore interface {
	Set(key string, value string)
	Get(key string) (value string)
	Del(key string)
}

// LSMKvStore LSMKvStore
type LSMKvStore struct {
	Index          *avl.Tree // 内存表
	ImmutableIndex *avl.Tree // 不可变的内存表
	SstTables      *sll.List // sstTable列表
	DataDir        string    // 目录
	Lock           sync.RWMutex
	StoreThreshold int64
	PartSize       int64
	WalFile        *os.File
}

// InitLSMKvStore InitLSMKvStore
func InitLSMKvStore(dataDir string, storeThreshold int64, partSize int64) (lSMKvStore *LSMKvStore, err error) {
	lSMKvStore = &LSMKvStore{
		DataDir:        dataDir,
		StoreThreshold: storeThreshold,
		PartSize:       partSize,
		SstTables:      sll.New(),
		Index:          avl.NewWithStringComparator(),
		ImmutableIndex: avl.NewWithStringComparator(),
		Lock:           sync.RWMutex{},
	}

	files, err := os.ReadDir(dataDir)
	if err != nil {
		files = make([]os.DirEntry, 0)
		err = os.Mkdir(dataDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	if len(files) == 0 {
		// dataDir + WAL
		filePath := dataDir + "wal"
		fileInfo, statErr := os.Stat(filePath)
		var walFile *os.File
		if statErr != nil {
			walFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				fmt.Printf("%v\n", err)
			}
		} else {
			walFile, err = os.OpenFile(filePath, os.O_APPEND|os.O_RDWR, 0666)
			if err != nil {
				fmt.Printf("%v\n", err)
			}
		}
		fmt.Printf("fileInfo %v\n", fileInfo)
		lSMKvStore.WalFile = walFile
		return lSMKvStore, nil
	}

	sstTreeMap := treemap.NewWithStringComparator()
	for _, file := range files {
		fileName := file.Name()
		absolutePath := dataDir + fileName

		// 从暂存的WAL中恢复数据，一般是持久化ssTable过程中
		// 异常才会留下walTmp
		if !file.IsDir() && fileName == "walTmp" {
			walFile, openErr := os.OpenFile(absolutePath, os.O_RDWR, 0666)
			if openErr != nil {
				fmt.Printf("%v\n", err)
			}
			lSMKvStore.restoreFromWal(walFile)
		}

		// 加载ssTable
		if !file.IsDir() && strings.HasSuffix(fileName, ".table") {
			// file name format is ${timestamp}.${suffix}
			fileNameSplit := strings.Split(fileName, ".")
			if len(fileNameSplit) > 0 {
				time := fileNameSplit[0]
				sst, createSStErr := CreateFromFile(absolutePath)
				if createSStErr != nil {
					fmt.Errorf("ParseInt err %v\n", createSStErr)
				}
				// 从小到大的将sstTable存入到排序树中
				sstTreeMap.Put(time, sst)
			}
		} else if !file.IsDir() && strings.HasSuffix(fileName, "wal") {
			// 加载wal文件
			walFile, openErr := os.OpenFile(absolutePath, os.O_RDWR, 0666)
			if openErr != nil {
				fmt.Printf("%v\n", err)
			}
			lSMKvStore.restoreFromWal(walFile)
			lSMKvStore.WalFile = walFile
		}
	}
	lSMKvStore.SstTables.Add(sstTreeMap.Values()...)
	return lSMKvStore, nil
}

// restoreFromWal 从暂存数据中恢复数据
func (kv *LSMKvStore) restoreFromWal(file *os.File) {
	fileStat, err := file.Stat()
	if err != nil {
		fmt.Errorf("get file stat err %v\n", err)
	}
	var start int64 = 0
	file.Seek(start, 0)
	for start < fileStat.Size() {
		intLenBuff := make([]byte, 4)
		file.Read(intLenBuff)
		valLen := BytesToInt32(intLenBuff)
		start = start + 4
		file.Seek(start, 0)
		valBytesBuf := make([]byte, valLen)
		file.Read(valBytesBuf)
		cmd := Cmd{}
		if err = json.Unmarshal(valBytesBuf, &cmd); err != nil {
			fmt.Errorf("Unmarshal err%v\n", err)
		}
		kv.Index.Put(cmd.Key, cmd)
		start = start + int64(valLen)
	}
	fileStat, err = file.Stat()
	if err != nil {
		fmt.Errorf("get file stat err %v\n", err)
	}
	file.Seek(fileStat.Size(), 0)
}

// Set set data to db
func (kv *LSMKvStore) Set(key string, value string) {
	kv.Lock.Lock()
	cmd := Cmd{
		Key:     key,
		Val:     value,
		CmdType: SET,
	}

	kv.writeWal(cmd)
	kv.Index.Put(key, cmd)
	kv.Lock.Unlock()

	// 当内存表的大小大于写存储阈值，则将内存中的数据刷到磁盘
	if int64(kv.Index.Size()) > kv.StoreThreshold {
		kv.switchIndex()
		kv.storeToSstTable()
	}
}

// Get get val
func (kv *LSMKvStore) Get(key string) (value string) {
	kv.Lock.RLock()
	defer kv.Lock.RUnlock()
	// 1 先从内存表中获取数据
	cmdObj, find := kv.Index.Get(key)
	if find {
		res := &Cmd{}
		jsonbyte, temerr := json.Marshal(cmdObj)
		if temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}

		if temerr = json.Unmarshal(jsonbyte, res); temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}

		if res.CmdType == DEL {
			return ""
		} else if res.CmdType == SET {
			return res.Val
		}
	}

	// 2 再从暂存表中获取
	cmdObj, find = kv.ImmutableIndex.Get(key)
	if find {
		res := &Cmd{}
		jsonbyte, temerr := json.Marshal(cmdObj)
		if temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}

		if temerr = json.Unmarshal(jsonbyte, res); temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}
		return res.Val
	}
	// 3 从sstTable中获取
	// it := kv.SstTables.Iterator()

	// sst table 问价是按照文件名字的时间顺序升序存在 SstTables 中
	// 因此 查询 SstTables 文件的时候应该倒序查询
	// 查询最新更新的key（最新的key覆盖旧值） 查询到则返回
	for index := kv.SstTables.Size() - 1; index >= 0; index-- {
		satval, findList := kv.SstTables.Get(index)
		if !findList {
			fmt.Errorf("get sst list err %v\n", findList)
		}

		sstTable := &SSTTable{}
		data, ok := (satval).(*SSTTable)
		if ok {
			sstTable = data
		}
		cmd, err := sstTable.query(key)
		if err != nil {
			fmt.Errorf("sstTable.query err %v\n", err)
			return ""
		}

		if cmd != nil && cmd.Key == key && cmd.CmdType == SET {
			return cmd.Val
		}

		// 数据被删除 直接返回空
		if cmd != nil && cmd.Key == key && cmd.CmdType == DEL {
			return ""
		}
	}
	return ""
}

// Del del val
func (kv *LSMKvStore) Del(key string) {
	kv.Lock.Lock()
	cmd := Cmd{
		Key:     key,
		CmdType: DEL,
	}
	kv.writeWal(cmd)
	kv.Index.Put(key, cmd)
	kv.Lock.Unlock()

	// 当内存表的大小大于写存储阈值，则将内存中的数据刷到磁盘
	if int64(kv.Index.Size()) > kv.StoreThreshold {
		kv.switchIndex()
		kv.storeToSstTable()
	}
}

// switchIndex 将达到存储阈值的内存表暂存，并生成新的wal文件与内存表
func (kv *LSMKvStore) switchIndex() {
	kv.Lock.Lock()
	defer kv.Lock.Unlock()

	tempValTree, err := kv.Index.ToJSON()
	if err != nil {
		fmt.Errorf("kv.Index.ToJSON() err%v\n", err)
	}
	kv.ImmutableIndex.FromJSON(tempValTree)
	kv.Index.Clear()

	walTmpFilePath := kv.DataDir + "walTmp"
	walFilePath := kv.DataDir + "wal"
	_, statErr := os.Stat(walTmpFilePath)
	if statErr != nil {
		fmt.Errorf("check file stat err %v\n", statErr)
	}

	if statErr == nil {
		// 如果文件存在 则删除文件
		err := os.Remove(walTmpFilePath)
		if err != nil {
			fmt.Errorf("delete walTmp file err %v\n", err)
		}
	}

	// 重命名文件 将 wal 重命名成 walTmp
	err = os.Rename(walFilePath, walTmpFilePath)
	if err != nil {
		fmt.Errorf("rename wal file err %v\n", err)
	}
}

// storeToSstTable 将暂存起来的数据落盘 写入到sstTable中
func (kv *LSMKvStore) storeToSstTable() {
	kv.Lock.Lock()
	defer kv.Lock.Unlock()

	now := time.Now().UnixNano() / int64(time.Millisecond)
	nowStr := strconv.FormatInt(now, 10)
	fileName := strings.Join([]string{nowStr, "table"}, ".")
	absolutePath := kv.DataDir + fileName
	logjsonBytes, err := kv.ImmutableIndex.ToJSON()
	if err != nil {
		fmt.Errorf("ImmutableIndex ToJSON err %v\n", err)
	}
	fmt.Printf("ImmutableIndex %v\n", string(logjsonBytes))
	sstTable, err := CreateFromIndex(absolutePath, int(kv.PartSize), *kv.ImmutableIndex)
	if err != nil {
		fmt.Errorf("CreateFromIndex err %v\n", err)
	}
	kv.SstTables.Add(sstTable)
	kv.ImmutableIndex.Clear()

	walTmpFilePath := kv.DataDir + "walTmp"
	_, statErr := os.Stat(walTmpFilePath)
	if statErr != nil {
		fmt.Errorf("check file stat err %v\n", statErr)
	}

	if statErr == nil {
		// 如果文件存在 则删除文件
		err := os.Remove(walTmpFilePath)
		if err != nil {
			fmt.Errorf("delete walTmp file err %v\n", err)
		}
	}

	walFilePath := kv.DataDir + "wal"
	walFile, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Errorf("open wal err %v\n", err)
	}
	kv.WalFile = walFile
}

func (kv *LSMKvStore) writeWal(cmd Cmd) {
	fileInfo, err := kv.WalFile.Stat()
	if err != nil {
		fmt.Errorf("check wal file stat err %v\n", err)
		return
	}
	fmt.Printf("wal file stat%v\n", fileInfo)

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		fmt.Errorf("Marshal get cmd err%v\n", err)
		return
	}
	cmdBytesLen := len(cmdBytes)
	cmdBytesLenBytes := Int32ToBytes(int32(cmdBytesLen))
	_, errWriteLen := kv.WalFile.Write(cmdBytesLenBytes)
	if errWriteLen != nil {
		fmt.Errorf("write data len to wal err %v\n", errWriteLen)
	}
	_, errWriteData := kv.WalFile.Write(cmdBytes)
	if errWriteData != nil {
		fmt.Errorf("write data to wal err %v\n", errWriteData)
	}

	// 将数据落盘
	flushErr := kv.WalFile.Sync()
	if flushErr != nil {
		fmt.Errorf("flush file err %v\n", flushErr)
	}
}

// Close close file data
func (kv *LSMKvStore) Close() {
	for _, sstVal := range kv.SstTables.Values() {
		sstTable := &SSTTable{}
		data, ok := (sstVal).(*SSTTable)
		if ok {
			sstTable = data
		}

		if sstTable == nil {
			return
		}

		sstTable.Close()
	}
}
