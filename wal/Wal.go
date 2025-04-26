package wal

import (
	"LSM/kv"
	"LSM/orderTable"
	"LSM/orderTable/skipList"
	"LSM/orderTable/sortTree"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
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
		elapsed := time.Since(start)
		log.Println("Loaded wal.log,Consumption of time :", elapsed)
	}()
	uuidStr := time.Now().Format("2006-01-02-15-04-05.000")
	walPath := path.Join(dir, fmt.Sprintf("%s_wal.log", uuidStr))
	log.Printf("init wal.log: walPath: %s\n", walPath)
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Println("The wal.log file cannot be created")
		panic(err)
	}
	w.f = f
	w.path = walPath
	w.lock = &sync.Mutex{}
}
func (w *Wal) LoadFromFile(path string, table orderTable.OrderInterface) orderTable.OrderInterface {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Println("The wal.log file cannot be opened")
		panic(err)
	}
	w.f = f
	w.path = path
	w.lock = &sync.Mutex{}
	return w.LoadToMemory(table)
}
func (w *Wal) LoadToMemory(table orderTable.OrderInterface) orderTable.OrderInterface {
	w.lock.Lock()
	defer w.lock.Unlock()
	var preTable orderTable.OrderInterface
	if _, ok := table.(*sortTree.Tree); ok {
		preTable = &sortTree.Tree{}
	} else if _, ok := table.(*skipList.SkipList); ok {
		preTable = &skipList.SkipList{}
	}
	//preTree := &sortTree.Tree{}
	preTable.Init()
	info, _ := os.Stat(w.path)
	size := info.Size()
	if size == 0 {
		return preTable
	}
	_, err := w.f.Seek(0, 0)
	if err != nil {
		log.Println("The wal.log file cannot be opened")
		panic(err)
	}
	defer func(f *os.File, offset int64, whence int) {
		_, err = f.Seek(offset, whence)
		if err != nil {
			log.Println("The wal.log file cannot be opened")
			panic(err)
		}
	}(w.f, size-1, 0)
	data := make([]byte, size)
	_, err = w.f.Read(data)
	if err != nil {
		log.Println("The wal.log file cannot be opened")
		panic(err)
	}
	dataLen := int64(0)
	index := int64(0)
	for index < size {
		indexData := data[index:(index + 8)]
		buf := bytes.NewBuffer(indexData)
		err = binary.Read(buf, binary.LittleEndian, &dataLen)
		if err != nil {
			log.Println("The wal.log file cannot be opened")
			panic(err)
		}
		index += 8
		dataArea := data[index:(index + dataLen)]
		var value kv.Value
		err = json.Unmarshal(dataArea, &value)
		if err != nil {
			log.Println("The wal.log file cannot be opened")
			panic(err)
		}
		if value.Delete {
			table.Delete(value.Key)
			preTable.Delete(value.Key)
		} else {
			table.Set(value.Key, value.Value)
			preTable.Set(value.Key, value.Value)
		}
		index += dataLen
	}
	return preTable
}
func (w *Wal) Write(value kv.Value) {
	w.lock.Lock()
	defer w.lock.Unlock()
	if value.Delete {
		log.Println("wal.log:	delete ", value.Key)
	} else {
		//log.Println("wal.log:	insert ", value.Key)
	}
	data, _ := json.Marshal(value)
	err := binary.Write(w.f, binary.LittleEndian, int64(len(data)))
	if err != nil {
		log.Println("The wal.log file cannot be written")
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

	log.Println("wal.log:	reset")
	err := w.f.Close()
	if err != nil {
		panic(err)
	}
	w.f = nil
	err = os.Remove(w.path)
	if err != nil {
		panic(err)
	}
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0666)
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
	err = os.Remove(w.path)
	if err != nil {
		log.Println("Failed to delete the wal.log")
		panic(err)
	}
}
