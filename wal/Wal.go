package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/whuanle/lsm/kv"
	"github.com/whuanle/lsm/sortTree"
	"log"
	"os"
	"path"
	"sync"
	"time"
)

type Wal struct {
	f    *os.File
	path string
	lock sync.Locker
}

func (w *Wal) Init(dir string) {
	log.Println("Loading wal.log...")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Loaded wal.log,Consumption of time : ", elapse)
	}()
	uuidStr := time.Now().Format("2006-01-02-15-04-05")
	walPath := path.Join(dir, fmt.Sprintf("%s_wal.log", uuidStr))
	log.Printf("init wal.log: walPath: %s\n", walPath)
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("The wal.log file cannot be created")
		panic(err)
	}
	w.f = f
	w.path = walPath
	w.lock = &sync.Mutex{}
}

func (w *Wal) LoadFromFile(path string, tree *sortTree.Tree) *sortTree.Tree {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("The wal.log file cannot be created")
		panic(err)
	}
	w.f = f
	w.path = path
	w.lock = &sync.Mutex{}
	return w.LoadToMemory(tree)
}

// LoadToMemory 会返回一个具有所有节点的Tree，并把节点的数据加载到参数的tree中
// 通过 wal.log 文件初始化 Wal，加载文件中的 WalF 到内存
func (w *Wal) LoadToMemory(tree *sortTree.Tree) *sortTree.Tree {
	w.lock.Lock()
	defer w.lock.Unlock()

	preTree := &sortTree.Tree{}
	preTree.Init()
	info, _ := os.Stat(w.path)
	size := info.Size()

	// 空的 wal.log
	if size == 0 {
		return preTree
	}

	_, err := w.f.Seek(0, 0)
	if err != nil {
		log.Println("Failed to open the wal.log")
		panic(err)
	}
	// 文件指针移动到最后，以便追加
	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Println("Failed to open the wal.log")
			panic(err)
		}
	}(w.f, size-1, 0)

	// 将文件内容全部读取到内存
	data := make([]byte, size)
	_, err = w.f.Read(data)
	if err != nil {
		log.Println("Failed to open the wal.log")
		panic(err)
	}

	dataLen := int64(0) // 元素的字节数量
	index := int64(0)   // 当前索引
	for index < size {
		// 前面的 8 个字节表示元素的长度
		indexData := data[index:(index + 8)]
		// 获取元素的字节长度
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &dataLen)
		if err != nil {
			log.Println("Failed to open the wal.log")
			panic(err)
		}
		// 将元素的所有字节读取出来，并还原为 kv.Value
		index += 8
		dataArea := data[index:(index + dataLen)]
		var value kv.Value
		err = json.Unmarshal(dataArea, &value)
		if err != nil {
			log.Println("Failed to open the wal.log")
			panic(err)
		}

		if value.Deleted {
			tree.Delete(value.Key)
			preTree.Delete(value.Key)
		} else {
			tree.Set(value.Key, value.Value)
			preTree.Set(value.Key, value.Value)
		}
		// 读取下一个元素
		index = index + dataLen
	}
	return preTree
}

// 记录日志
func (w *Wal) Write(value kv.Value) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if value.Deleted {
		log.Println("wal.log:	delete ", value.Key)
	} else {
		log.Println("wal.log:	insert ", value.Key)
	}

	data, _ := json.Marshal(value)
	err := binary.Write(w.f, binary.LittleEndian, int64(len(data)))
	if err != nil {
		log.Println("Failed to write the wal.log")
		panic(err)
	}

	err = binary.Write(w.f, binary.LittleEndian, data)
	if err != nil {
		log.Println("Failed to write the wal.log")
		panic(err)
	}
}

func (w *Wal) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()

	log.Println("Resetting the wal.log file")

	err := w.f.Close()
	if err != nil {
		panic(err)
	}
	w.f = nil
	err = os.Remove(w.path)
	if err != nil {
		panic(err)
	}
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	w.f = f
}

func (w *Wal) DeleteFile() {
	w.lock.Lock()
	defer w.lock.Unlock()
	log.Printf("Deleting the wal.log file: %s\n", w.path)
	err := w.f.Close()
	if err != nil {
		panic(err)
	}
	_ = os.Remove(w.path)
}
