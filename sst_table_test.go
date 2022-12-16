package mitkv

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	avl "github.com/emirpasic/gods/trees/avltree"
)

func TestWriteFile(t *testing.T) {
	sstMeta := SSTTableMetaInfo{
		Version:    1001,
		DataStart:  1002,
		DataLen:    1003,
		IndexStart: 1004,
		IndexLen:   1005,
		PartSize:   1006,
	}
	// 可写方式打开文件
	file, err := os.OpenFile(
		"test.txt",
		os.O_APPEND|os.O_WRONLY|os.O_TRUNC|os.O_CREATE,
		0666,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	// 写字节到文件中
	versionByte := Int64ToBytes(sstMeta.Version)
	//byteSlice := []byte("Bytes!\n")
	bytesWritten, err := file.Write(versionByte)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(bytesWritten)

	dataStartByte := Int64ToBytes(sstMeta.DataStart)
	bytesWritten, err = file.Write(dataStartByte)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(bytesWritten)

	dataLenByte := Int64ToBytes(sstMeta.DataLen)
	bytesWritten, err = file.Write(dataLenByte)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(bytesWritten)

	indexStartByte := Int64ToBytes(sstMeta.IndexStart)
	bytesWritten, err = file.Write(indexStartByte)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(bytesWritten)

	indexLenByte := Int64ToBytes(sstMeta.IndexLen)
	bytesWritten, err = file.Write(indexLenByte)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(bytesWritten)

	partSizeByte := Int64ToBytes(sstMeta.PartSize)
	bytesWritten, err = file.Write(partSizeByte)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(bytesWritten)
}

func TestReadFile(t *testing.T) {
	sstMeta := SSTTableMetaInfo{}
	// Open file for reading
	file, err := os.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	// file.Read()可以读取一个小文件到大的byte slice中，
	// 但是io.ReadFull()在文件的字节数小于byte slice字节数的时候会返回错误
	// 从文件中读取len(b)字节的文件。
	// 返回0字节意味着读取到文件尾了
	// 读取到文件会返回io.EOF的error
	byteSlice := make([]byte, 8)
	bytesRead, err := file.Read(byteSlice)
	if err != nil {
		t.Fatal(err)
	}
	sstMeta.Version = BytesToInt64(byteSlice)

	t.Log(sstMeta.Version)
	t.Log(bytesRead)

	file.Seek(8, 0)
	byteSlice = make([]byte, 8)
	bytesRead, err = file.Read(byteSlice)
	if err != nil {
		t.Fatal(err)
	}
	sstMeta.DataStart = BytesToInt64(byteSlice)
	t.Log(sstMeta.DataStart)
	t.Log(bytesRead)

	file.Seek(16, 0)
	byteSlice = make([]byte, 8)
	bytesRead, err = file.Read(byteSlice)
	if err != nil {
		t.Fatal(err)
	}
	sstMeta.DataLen = BytesToInt64(byteSlice)
	t.Log(sstMeta.DataLen)
	t.Log(bytesRead)

	file.Seek(24, 0)
	byteSlice = make([]byte, 8)
	bytesRead, err = file.Read(byteSlice)
	if err != nil {
		t.Fatal(err)
	}
	sstMeta.IndexStart = BytesToInt64(byteSlice)
	t.Log(sstMeta.IndexStart)
	t.Log(bytesRead)

	file.Seek(32, 0)
	byteSlice = make([]byte, 8)
	bytesRead, err = file.Read(byteSlice)
	if err != nil {
		t.Fatal(err)
	}
	sstMeta.IndexLen = BytesToInt64(byteSlice)
	t.Log(sstMeta.IndexLen)
	t.Log(bytesRead)

	file.Seek(40, 0)
	byteSlice = make([]byte, 8)
	bytesRead, err = file.Read(byteSlice)
	if err != nil {
		t.Fatal(err)
	}

	sstMeta.PartSize = BytesToInt64(byteSlice)
	t.Log(sstMeta.PartSize)
	t.Log(bytesRead)
}

func TestAvlTree(t *testing.T) {
	tree := avl.NewWithStringComparator() // empty(keys are of type int)

	tree.Put("1", "x") // 1->x
	tree.Put("2", "b") // 1->x, 2->b (in order)
	tree.Put("1", "a") // 1->a, 2->b (in order, replacement)
	tree.Put("3", "c") // 1->a, 2->b, 3->c (in order)
	tree.Put("4", "d") // 1->a, 2->b, 3->c, 4->d (in order)
	tree.Put("5", "e") // 1->a, 2->b, 3->c, 4->d, 5->e (in order)
	tree.Put("6", "f") // 1->a, 2->b, 3->c, 4->d, 5->e, 6->f (in order)
	fmt.Println(tree)
}

func TestCreateFromIndex(t *testing.T) {
	indexTree := avl.NewWithStringComparator()
	for i := 0; i < 10; i++ {
		cmd := Cmd{
			Key:     "key" + strconv.Itoa(i),
			Val:     "value" + strconv.Itoa(i),
			CmdType: SET,
		}
		indexTree.Put(cmd.Key, cmd)
	}

	CreateFromIndex("test.txt", 3, *indexTree)
}

func TestCreateFromFile(t *testing.T) {
	CreateFromFile("test.txt")
}

func TestQuery(t *testing.T) {
	sst, err := CreateFromFile("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(sst.query("key0"))
	t.Log(sst.query("key1"))
	t.Log(sst.query("key2"))
	t.Log(sst.query("key3"))

	t.Log(sst.query("key11111"))
}
