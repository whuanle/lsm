package ssTable

import (
	"LSM/config"
	"LSM/kv"
	"LSM/orderTable/skipList"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"time"
)

func (tree *TableTree) Check() {
	tree.majorCompaction()
}
func (tree *TableTree) majorCompaction() {
	con := config.GetConfig()
	for levelIndex, _ := range tree.levels {
		//fmt.Println("-------cmp------", tree.getLevelCount(levelIndex), con.PartSize*int(math.Pow(10, float64(levelIndex))))
		if tree.getLevelCount(levelIndex) > con.PartSize*int(math.Pow(10, float64(levelIndex))) {
			tree.majorCompactionLevel(levelIndex)
		}
	}
}
func GetTableValues(table *SSTable) []kv.Value {
	tableCache := make([]byte, 100000)
	memoryList := skipList.SkipList{}
	memoryList.Init()
	if int64(len(tableCache)) < table.tableMetaInfo.dataLen {
		tableCache = make([]byte, table.tableMetaInfo.dataLen)
	}
	newSlice := tableCache[0:table.tableMetaInfo.dataLen]
	if _, err := table.f.Seek(0, 0); err != nil {
		log.Println("error open file", table.filePath)
		panic(err)
	}
	if _, err := table.f.Read(newSlice); err != nil {
		log.Println("error read file", table.filePath)
		panic(err)
	}
	for k, position := range table.sparseIndex {
		if position.Deleted == false {
			value, err := kv.Decode(newSlice[position.Start:(position.Start + position.Len)])
			if err != nil {
				log.Fatal(err)
			}
			memoryList.Set(k, value.Value)
		} else {
			memoryList.Delete(k)
		}
	}
	return memoryList.GetValues()
}
func (tree *TableTree) majorCompactionLevel(level int) {
	log.Println("Compressing layer ", level, " files")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Completed compression,consumption of time : ", elapse)
	}()
	con := config.GetConfig()
	if level == 0 {
		tree.levellocks[0].Lock()
		tree.levellocks[1].Lock()
		tableCache := make([]byte, levelMaxSize[level])
		currentNode := tree.levels[level]
		memoryList := skipList.SkipList{}
		memoryList.Init()
		for currentNode != nil {
			table := currentNode.table
			if int64(len(tableCache)) < table.tableMetaInfo.dataLen {
				tableCache = make([]byte, table.tableMetaInfo.dataLen)
			}
			newSlice := tableCache[0:table.tableMetaInfo.dataLen]
			if _, err := table.f.Seek(0, 0); err != nil {
				log.Println("error open file", table.filePath)
				panic(err)
			}
			if _, err := table.f.Read(newSlice); err != nil {
				log.Println("error read file", table.filePath)
				panic(err)
			}
			for k, position := range table.sparseIndex {
				if position.Deleted == false {
					value, err := kv.Decode(newSlice[position.Start:(position.Start + position.Len)])
					if err != nil {
						log.Fatal(err)
					}
					memoryList.Set(k, value.Value)
				} else {
					memoryList.Delete(k)
				}
			}
			currentNode.needToDelete = true
			currentNode = currentNode.next
		} //已经将第一层的数据放进跳表里
		memoryValue := memoryList.GetValues()
		//log.Println("-----------the last", memoryValue[len(memoryValue)-1].Key)
		nowMemoryR := memoryValue[len(memoryValue)-1].Key
		tableCache = make([]byte, levelMaxSize[level+1])
		currentNode = tree.levels[level+1]
		nowPtr := 0
		mergeToList := skipList.SkipList{}
		mergeToList.Init()
		mergeToSize := 0
		var mergeValue []kv.Value
		for currentNode != nil || nowPtr < len(memoryValue) {
			if currentNode != nil {
				if currentNode.tableNodeL > nowMemoryR || currentNode.tableNodeR < memoryValue[nowPtr].Key {
					currentNode = currentNode.next
					continue
				}
			}
			if currentNode != nil {
				mergeValue = GetTableValues(currentNode.table)
			} else {
				break
				mergeValue = make([]kv.Value, 0)
			}
			for i := 0; i < len(mergeValue); {
				value := mergeValue[i]
				if nowPtr == len(memoryValue) {
					if !value.Delete {
						mergeToList.Set(value.Key, value.Value)
					} else {
						mergeToList.Delete(value.Key)
					}
					i++
				} else if memoryValue[nowPtr].Key < value.Key {
					if !memoryValue[nowPtr].Delete {
						mergeToList.Set(memoryValue[nowPtr].Key, memoryValue[nowPtr].Value)
					} else {
						mergeToList.Delete(memoryValue[nowPtr].Key)
					}
					nowPtr++
				} else if memoryValue[nowPtr].Key > value.Key {
					if !value.Delete {
						mergeToList.Set(value.Key, value.Value)
					} else {
						mergeToList.Delete(value.Key)
					}
					i++
				} else {
					if !memoryValue[nowPtr].Delete {
						mergeToList.Set(value.Key, value.Value)
					} else {
						mergeToList.Delete(value.Key)
					}
					i++
					nowPtr++
				}
				mergeToSize++
				if mergeToSize >= con.Threshold {
					tree.creatTable(mergeToList.Swap().GetValues(), level+1)
					mergeToSize = 0
				}
			}
			currentNode.needToDelete = true
			currentNode = currentNode.next
		}
		for nowPtr < len(memoryValue) {
			if !memoryValue[nowPtr].Delete {
				mergeToList.Set(memoryValue[nowPtr].Key, memoryValue[nowPtr].Value)
			} else {
				mergeToList.Delete(memoryValue[nowPtr].Key)
			}
			nowPtr++
			mergeToSize++
			if mergeToSize >= con.Threshold {
				//fmt.Println(mergeToSize, "\n", "\n")
				tree.creatTable(mergeToList.Swap().GetValues(), level+1)
				mergeToSize = 0
			}
		}
		if mergeToSize != 0 {
			tree.creatTable(mergeToList.Swap().GetValues(), level+1)
			mergeToSize = 0
		}
		tree.clearLevel(0)
		tree.levellocks[0].Unlock()
		tree.levellocks[1].Unlock()
	} else {
		tree.levellocks[level].Lock()
		tree.levellocks[level+1].Lock()
		upCurrentNode := tree.levels[level]
		downCurrentNode := tree.levels[level+1]
		mergeToList := skipList.SkipList{}
		mergeToList.Init()
		mergeToSize := 0
		tmpNode := tree.levels[level]
		upL := upCurrentNode.tableNodeL
		for tmpNode.next != nil {
			tmpNode = tmpNode.next
		}
		upR := tmpNode.tableNodeR
		var upValues []kv.Value
		var downValues []kv.Value
		for downCurrentNode != nil {
			if downCurrentNode.tableNodeR > upL || downCurrentNode.next == nil {
				break
			}
			downCurrentNode = downCurrentNode.next
		}
		upPtr := 0
		downPtr := 0
		upValues = GetTableValues(upCurrentNode.table)
		if downCurrentNode != nil {
			downValues = GetTableValues(downCurrentNode.table)
		}
		for upCurrentNode != nil || downCurrentNode != nil {
			if upPtr == len(upValues) {
				upCurrentNode.needToDelete = true
				upCurrentNode = upCurrentNode.next
				if upCurrentNode != nil {
					upPtr = 0
					upValues = GetTableValues(upCurrentNode.table)
				} else {
					upPtr = -1
				}
			}
			if downPtr == len(downValues) {
				if downCurrentNode != nil {
					downCurrentNode.needToDelete = true
					downCurrentNode = downCurrentNode.next
					if downCurrentNode != nil || downCurrentNode.tableNodeL <= upR {
						downPtr = 0
						downValues = GetTableValues(downCurrentNode.table)
					} else {
						downPtr = -1
					}
				} else {
					downPtr = -1
				}

			}
			if upPtr == -1 && downPtr == -1 {
				break
			}
			if downPtr == -1 || upValues[upPtr].Key == downValues[downPtr].Key || upValues[upPtr].Key < downValues[downPtr].Key {
				if !upValues[upPtr].Delete {
					mergeToList.Set(upValues[upPtr].Key, upValues[upPtr].Value)
				} else {
					mergeToList.Delete(upValues[upPtr].Key)
				}
				mergeToSize++
				upPtr++
			} else if upPtr == -1 || upValues[upPtr].Key > downValues[downPtr].Key {
				if !upValues[upPtr].Delete {
					mergeToList.Set(downValues[downPtr].Key, downValues[downPtr].Value)
				} else {
					mergeToList.Delete(downValues[downPtr].Key)
				}
				mergeToSize++
				downPtr++
			}
			if mergeToSize >= con.Threshold {
				tree.creatTable(mergeToList.Swap().GetValues(), level+1)
				mergeToSize = 0
			}
		}
		if mergeToSize != 0 {
			tree.creatTable(mergeToList.Swap().GetValues(), level+1)
			mergeToSize = 0
		}
		tree.clearLevel(level)
		tree.clearLevel(level + 1)
		tree.levellocks[level].Unlock()
		tree.levellocks[level+1].Unlock()
	}
	//tree.levels[level] = nil
}

func (tree *TableTree) clearLevel(level int) {
	log.Println("Clear level", level)
	currentNode := tree.levels[level]
	tmpNodes := make([]*tableNode, 0)
	for currentNode != nil {
		if currentNode.needToDelete == false {
			tmpNodes = append(tmpNodes, currentNode)
		} else {
			err := currentNode.table.f.Close()
			if err != nil {
				log.Println("error close file,", err)
				panic(err)
			}
			err = os.Remove(currentNode.table.filePath)
			if err != nil {
				log.Println("error remove file,", currentNode.table.filePath, err)
				panic(err)
			}
			currentNode.table.f = nil
			currentNode.table = nil
		}
		currentNode = currentNode.next
	}
	sort.Slice(tmpNodes, func(i, j int) bool {
		return tmpNodes[i].tableNodeL < tmpNodes[j].tableNodeL
	})
	if len(tmpNodes) == 0 {
		tree.levels[level] = nil
		return
	}
	for i := 1; i < len(tmpNodes); i++ {
		tmpNodes[i-1].next = tmpNodes[i]
	}
	tmpNodes[len(tmpNodes)-1].next = nil
	tree.levels[level] = tmpNodes[0]
	for i := 0; i < len(tmpNodes); i++ {
		os.Rename(tmpNodes[i].table.filePath, config.Config{}.DataDir+"/"+strconv.Itoa(level)+"."+strconv.Itoa(i)+".db")
	}
	log.Println("Cleared level leave", tree.getLevelCount(level))
	return
}
