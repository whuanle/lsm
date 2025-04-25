package ssTable

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func (tree *TableTree) loadDbFile(path string) {
	log.Println("Loading the ", path)
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Loading the ", path, ",Consumption of time : ", elapse)
	}()
	level, index, err := getLevel(filepath.Base(path))
	if err != nil {
		return
	}
	table := &SSTable{}
	table.Init(path)
	newNode := &tableNode{
		index: index,
		table: table,
	}
	currentNode := tree.levels[level]
	if currentNode == nil {
		tree.levels[level] = newNode
		newNode.next = nil
		return
	}
	if currentNode.index > newNode.index {
		newNode.next = currentNode
		tree.levels[level] = newNode
		return
	}
	for currentNode.next != nil {
		if currentNode.next == nil || newNode.index < currentNode.next.index {
			newNode.next = currentNode.next
			currentNode.next = newNode
			break
		} else {
			currentNode = currentNode.next
		}
	}
}
func (table *SSTable) loadFileHandle() {
	if table.f == nil {
		f, err := os.OpenFile(table.filePath, os.O_RDONLY, 0666)
		if err != nil {
			log.Println("Error opening file ", table.filePath)
		}
		table.f = f
	}
	table.loadMetaInfo()
	table.loadSparseIndex()
}
func (table *SSTable) loadMetaInfo() {
	f := table.f
	_, err := f.Seek(0, 0)
	if err != nil {
		log.Println("Error seeking file ", table.filePath)
		panic(err)
	}
	info, _ := f.Stat()
	_, err = f.Seek(info.Size()-8*5, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.version)
	_, err = f.Seek(info.Size()-8*4, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.dataStart)

	_, err = f.Seek(info.Size()-8*3, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.dataLen)

	_, err = f.Seek(info.Size()-8*2, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.indexStart)

	_, err = f.Seek(info.Size()-8*1, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.filePath)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.tableMetaInfo.indexLen)
}
func (table *SSTable) loadSparseIndex() {
	bytes := make([]byte, table.tableMetaInfo.indexLen)
	if _, err := table.f.Seek(table.tableMetaInfo.indexStart, 0); err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	if _, err := table.f.Read(bytes); err != nil {
		log.Println("error open file ", table.filePath)
		panic(err)
	}
	table.sparseIndex = make(map[string]Position)
	err := json.Unmarshal(bytes, &table.sparseIndex)
	if err != nil {
		log.Println(" error open file ", table.filePath)
		panic(err)
	}
	_, _ = table.f.Seek(0, 0)
	keys := make([]string, 0, len(table.sparseIndex))
	for k := range table.sparseIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	table.sortIndex = keys
}
