package mitkv

import (
	"encoding/json"
	"fmt"
	sll "github.com/emirpasic/gods/lists/singlylinkedlist"
	"github.com/emirpasic/gods/maps/treemap"
	avl "github.com/emirpasic/gods/trees/avltree"
	"os"
)

type SSTTable struct {
	MetaInfo    SSTTableMetaInfo
	SparseIndex *avl.Tree
	TableFile   *os.File
	FilePath    string
}

// initFromIndex 从内存中的avlTree生成ssTable
func (sst *SSTTable) initFromIndex(index avl.Tree) {
	tableFileStat, err := sst.TableFile.Stat()
	if err != nil {
		fmt.Errorf("get file stat err :%v\n", err)
	}
	sst.MetaInfo.DataStart = tableFileStat.Size()
	// 如何实现有序的map
	partData := treemap.NewWithStringComparator()
	it := index.Iterator()
	for it.Next() {
		keyStr := Value2Str(it.Key())
		valStruct := &Cmd{}
		jsonbyte, temerr := json.Marshal(it.Value())
		if temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}

		if temerr = json.Unmarshal(jsonbyte, valStruct); temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}
		partData.Put(keyStr, valStruct)
		partSize := int(sst.MetaInfo.PartSize)
		if partData.Size() >= partSize {
			sst.writeDataPart(partData)
		}
	}

	// 将遍历完剩余的数据继续写入文件
	if partData.Size() > 0 {
		sst.writeDataPart(partData)
	}

	// 重新获取下当前文件状态
	tableFileStat, err = sst.TableFile.Stat()
	if err != nil {
		fmt.Errorf("get file stat err :%v\n", err)
	}
	dataPartLen := tableFileStat.Size() - sst.MetaInfo.DataStart
	sst.MetaInfo.DataLen = dataPartLen

	// 保存稀疏索引
	indexBytes, err := sst.SparseIndex.ToJSON()
	if err != nil {
		fmt.Errorf("josn Marshal err %v\n", err)
	}
	sst.MetaInfo.IndexStart = tableFileStat.Size()
	_, sstWriteErr := sst.TableFile.Write(indexBytes)
	if sstWriteErr != nil {
		fmt.Errorf("sst table write err%v\n", sstWriteErr)
	}
	sstSyncErr := sst.TableFile.Sync()
	if sstSyncErr != nil {
		fmt.Errorf("sync sst table file err %v\n", sstSyncErr)
	}

	sst.MetaInfo.IndexLen = int64(len(indexBytes))
	fmt.Printf("SparseIndex is %v\n", sst.SparseIndex)

	// 保存文件索引
	sst.MetaInfo.writeToFile(sst.TableFile)
	fmt.Printf("file_path = %v, mate_info = %v\n", sst.FilePath, sst.MetaInfo)
}

// restoreFromFile 应用突然宕机，在服务重启时，从文件中重新构建sstTable
func (sst *SSTTable) restoreFromFile() {
	metaInfo, err := sst.MetaInfo.readFromFile(sst.TableFile)
	if err != nil {
		fmt.Errorf("meta readFromFile err %v\n", err)
		return
	}
	// TOOD 多次写ssttable的问题 如何解析
	fmt.Printf("restoreFromFile start ......\n")
	indexBytes := make([]byte, int(metaInfo.IndexLen))
	sst.TableFile.Seek(metaInfo.IndexStart, 0)
	sst.TableFile.Read(indexBytes)

	fmt.Printf("index ssttable data %v\n", string(indexBytes))
	if err := sst.SparseIndex.FromJSON(indexBytes); err != nil {
		fmt.Errorf("json Unmarshal err %v\n", err)
	}
	fmt.Printf("SparseIndex = %v\n", sst.SparseIndex)
	sst.MetaInfo = metaInfo
}

func (sst *SSTTable) writeDataPart(partData *treemap.Map) {
	partDataBytes, err := partData.ToJSON()
	if err != nil {
		fmt.Errorf("json Marshal err %v\n", err)
	}
	filestat, err := sst.TableFile.Stat()
	if err != nil {
		fmt.Errorf("get file stat err %v\n", err)
	}
	start := filestat.Size()

	_, sstWriteErr := sst.TableFile.Write(partDataBytes)
	if sstWriteErr != nil {
		fmt.Errorf("sst table write file err%v\n", sstWriteErr)
	}

	syncSstErr := sst.TableFile.Sync()
	if syncSstErr != nil {
		fmt.Errorf("sync sst table file err %v\n", syncSstErr)
	}

	// 获取最小的key索引，用来做稀疏索引，
	k, _ := partData.Min()
	sst.SparseIndex.Put(k, Position{
		Start: start,
		Len:   int64(len(partDataBytes)),
	})
	partData.Clear()
}

func (sst *SSTTable) Close() {
	sst.TableFile.Close()
}

// query 从sstTable中查询数据，查询逻辑是 从内存中查找 -> 从最新的sstTable中查找 -> 从老的sstTable中查找
// 查询过程是从索引中获取到key对应的最上下限的position，
func (sst *SSTTable) query(key string) (cmd *Cmd, err error) {
	list := sll.New()
	lastSmallPos := &Position{}
	fitstBigPos := &Position{}

	// 从索引表中找到最后一个小于key的位置，以及第一个大于key的位置
	for _, k := range sst.SparseIndex.Keys() {
		keyStr := Value2Str(k)
		if keyStr <= key {
			tempPos, isFind := sst.SparseIndex.Get(k)
			if isFind {
				jsonbyte, temerr := json.Marshal(tempPos)
				if temerr != nil {
					fmt.Errorf("err %v\n", temerr)
				}

				if temerr = json.Unmarshal(jsonbyte, lastSmallPos); temerr != nil {
					fmt.Errorf("err %v\n", temerr)
				}
			}
		} else {
			tempPos, isFind := sst.SparseIndex.Get(k)
			if isFind {
				jsonbyte, temerr := json.Marshal(tempPos)
				if temerr != nil {
					fmt.Errorf("err %v\n", temerr)
				}

				if temerr = json.Unmarshal(jsonbyte, fitstBigPos); temerr != nil {
					fmt.Errorf("err %v\n", temerr)
				}
			}
			break
		}
	}

	if lastSmallPos.Start >= 0 && lastSmallPos.Len > 0 {
		list.Add(lastSmallPos)
	}

	if fitstBigPos.Start >= 0 && fitstBigPos.Len > 0 {
		list.Add(fitstBigPos)
	}

	if list.Size() == 0 {
		return
	}

	fitstKeyPos := &Position{}
	lastKeyPos := &Position{}
	temFirstPos, isFind := list.Get(0)
	if isFind {
		//pos, ok := temFirstPos.(Position)
		//if ok {
		//	fitstKeyPos = &pos
		//}
		jsonbyte, temerr := json.Marshal(temFirstPos)
		if temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}

		if temerr = json.Unmarshal(jsonbyte, fitstKeyPos); temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}
	}

	lastPos, isFind := list.Get(list.Size() - 1)
	if isFind {
		jsonbyte, temerr := json.Marshal(lastPos)
		if temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}

		if temerr = json.Unmarshal(jsonbyte, lastKeyPos); temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}
	}

	var length int64 = 0
	start := fitstKeyPos.Start
	if fitstKeyPos.Start == lastKeyPos.Start && fitstKeyPos.Len == lastKeyPos.Len {
		length = fitstKeyPos.Len
	} else {
		length = lastKeyPos.Start + lastKeyPos.Len - start
	}

	dataPartBytes := make([]byte, int(length))
	sst.TableFile.Seek(start, 0)
	sst.TableFile.Read(dataPartBytes)
	var pStart int64 = 0
	it := list.Iterator()
	for it.Next() {
		innerPos := Position{}
		jsonbyte, temerr := json.Marshal(it.Value())
		if temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}

		if temerr = json.Unmarshal(jsonbyte, &innerPos); temerr != nil {
			fmt.Errorf("err %v\n", temerr)
		}

		partData := treemap.NewWithStringComparator()
		if err := partData.FromJSON(dataPartBytes[pStart:int(innerPos.Len)]); err != nil {
			fmt.Errorf("treemap from json err %v", err)
		}
		data, findKey := partData.Get(key)
		if findKey {
			res := &Cmd{}
			jsonbyte, temerr = json.Marshal(data)
			if temerr != nil {
				fmt.Errorf("err %v\n", temerr)
			}

			if temerr = json.Unmarshal(jsonbyte, res); temerr != nil {
				fmt.Errorf("err %v\n", temerr)
			}

			return res, nil
		}
		pStart = pStart + innerPos.Len
	}

	return nil, err
}

func InitSstTable(filePath string, partSize int) (sst *SSTTable, err error) {
	sst = &SSTTable{}
	sst.MetaInfo = SSTTableMetaInfo{}
	sst.MetaInfo.PartSize = int64(partSize)
	sst.FilePath = filePath

	var file *os.File
	fileInfo, err := os.Stat(filePath)
	if err != nil && os.IsNotExist(err) {
		file, err = os.OpenFile(
			filePath,
			os.O_APPEND|os.O_RDWR|os.O_CREATE,
			0666,
		)
		if err != nil {
			return nil, err
		}
	} else {
		file, err = os.OpenFile(
			filePath,
			os.O_APPEND|os.O_RDWR,
			0666,
		)
		if err != nil {
			return nil, err
		}
	}

	fmt.Printf("fileInfo = %v\n", fileInfo)
	sst.TableFile = file
	sst.SparseIndex = avl.NewWithStringComparator()
	return sst, err
}

func CreateFromIndex(filePath string, partSize int, index avl.Tree) (sst *SSTTable, err error) {
	sst, err = InitSstTable(filePath, partSize)
	if err != nil {
		return sst, err
	}
	sst.initFromIndex(index)
	return sst, err
}

func CreateFromFile(filePath string) (sst *SSTTable, err error) {
	sst, err = InitSstTable(filePath, 0)
	if err != nil {
		return sst, err
	}
	sst.restoreFromFile()
	return sst, err
}

// SSTTableMetaInfo sstTable的元数据
type SSTTableMetaInfo struct {
	Version    int64 // 版本号
	DataStart  int64 //数据区开始
	DataLen    int64 // 数据区长度
	IndexStart int64 // 索引区开始
	IndexLen   int64 // 索引区长度
	PartSize   int64 // 分段的大小
}

func (sstMeta *SSTTableMetaInfo) writeToFile(file *os.File) {
	// file 是通过append的方式来打开或者创建的文件
	file.Write(Int64ToBytes(sstMeta.Version))
	file.Write(Int64ToBytes(sstMeta.DataStart))
	file.Write(Int64ToBytes(sstMeta.DataLen))
	file.Write(Int64ToBytes(sstMeta.IndexStart))
	file.Write(Int64ToBytes(sstMeta.IndexLen))
	file.Write(Int64ToBytes(sstMeta.PartSize))
	syncFileErr := file.Sync()
	if syncFileErr != nil {
		fmt.Errorf("syncFileErr of write meta file %v\n", syncFileErr)
	}
}

func (sstMeta *SSTTableMetaInfo) readFromFile(file *os.File) (sstMetaRes SSTTableMetaInfo, err error) {
	sstMetaRes = SSTTableMetaInfo{}

	stat, err := file.Stat()
	if err != nil {
		fmt.Errorf("err = %v\n", err)
		return sstMetaRes, err
	}

	file.Seek(stat.Size()-8, 0)
	byteSlice := make([]byte, 8)
	_, err = file.Read(byteSlice)
	sstMetaRes.PartSize = BytesToInt64(byteSlice)

	file.Seek(stat.Size()-16, 0)
	byteSlice = make([]byte, 8)
	_, err = file.Read(byteSlice)
	sstMetaRes.IndexLen = BytesToInt64(byteSlice)

	file.Seek(stat.Size()-24, 0)
	byteSlice = make([]byte, 8)
	_, err = file.Read(byteSlice)
	sstMetaRes.IndexStart = BytesToInt64(byteSlice)

	file.Seek(stat.Size()-32, 0)
	byteSlice = make([]byte, 8)
	_, err = file.Read(byteSlice)
	sstMetaRes.DataLen = BytesToInt64(byteSlice)

	file.Seek(stat.Size()-40, 0)
	byteSlice = make([]byte, 8)
	_, err = file.Read(byteSlice)
	sstMetaRes.DataStart = BytesToInt64(byteSlice)

	file.Seek(stat.Size()-48, 0)
	byteSlice = make([]byte, 8)
	_, err = file.Read(byteSlice)
	sstMetaRes.Version = BytesToInt64(byteSlice)

	return sstMetaRes, err
}

const (
	SET = iota
	GET
	DEL
)

type Cmd struct {
	Key     string
	Val     string
	CmdType int
}

type Position struct {
	Start int64
	Len   int64
}
