package ssTable

import (
	"encoding/binary"
	"log"
	"os"
)

/*
管理 SSTable 的磁盘文件
*/

// GetDbSize 获取 .db 数据文件大小
func (table *SSTable) GetDbSize() int64 {
	info, err := os.Stat(table.filePath)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}

// GetLevelSize 获取指定层的 SSTable 总大小
func (tree *TableTree) GetLevelSize(level int) int64 {
	var size int64
	node := tree.levels[level]
	for node != nil {
		size += node.table.GetDbSize()
		node = node.next
	}
	return size
}

// 将数据写入文件
func writeDataToFile(filePath string, dataArea []byte, indexArea []byte, meta MetaInfo) {
	f, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(" error create file,", err)
	}
	_, err = f.Write(dataArea)
	if err != nil {
		log.Fatal(" error write file,", err)
	}
	_, err = f.Write(indexArea)
	if err != nil {
		log.Fatal(" error write file,", err)
	}
	// 写入元数据到文件末尾
	// 注意，右侧必须能够识别字节长度的类型，不能使用 int 这种类型，只能使用 int32、int64 等
	_ = binary.Write(f, binary.LittleEndian, &meta.version)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.dataLen)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexStart)
	_ = binary.Write(f, binary.LittleEndian, &meta.indexLen)
	err = f.Sync()
	if err != nil {
		log.Fatal(" error write file,", err)
	}
	err = f.Close()
	if err != nil {
		log.Fatal(" error close file,", err)
	}
}
