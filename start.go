package lsm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/whuanle/lsm/config"
	"github.com/whuanle/lsm/kv"
	"github.com/whuanle/lsm/memory"
	"github.com/whuanle/lsm/ssTable"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sync"
)

// Start 启动数据库
func Start(con config.Config) {
	if db != nil {
		return
	}
	// 将配置保存到内存中
	config.Init(con)

	// 初始化数据库
	initDatabase(con.DataDir)
	// 启动后台线程
	go Check()
}

// 初始化 Database
func initDatabase(dir string) {
	// 从磁盘文件中还原 SsTable、WalF、内存表等
	tables := ssTable.Init(10) // 固定最大 10 层
	database := &Database{
		MemoryTree: &memory.SortTree{},
		TableTree:  tables,
		MemoryLock: &sync.RWMutex{},
	}
	db = database
	// 加载 WalF、db 文件
	loadDataFile(dir)
}

// 从数据目录中，加载 WalF、db 文件
func loadDataFile(dir string) {
	// 如果目录不存在，则为空数据库
	if _, err := os.Stat(dir); err != nil {
		err := os.Mkdir(dir, 0666)
		if err != nil {
			log.Fatalln("Failed to create the database directory,", err)
		}
		// 创建 WalF 文件
		walF, err := os.OpenFile(path.Join(dir, "WalF.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalln("Failed to create the WalF.log,", err)
		}
		db.WalF = walF
		return
	}

	fs, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatalln("Failed to read the database file,", err)
	}

	// 加载 WalF 和 SsTable 文件
	for _, info := range fs {
		if info.IsDir() {
			continue
		}
		// 如果是 SsTable 文件
		if path.Ext(info.Name()) == ".db" {
			db.TableTree.LoadDbFile(path.Join(dir, info.Name()))
		} else if info.Name() == "WalF.log" {
			// 如果有 WalF 文件，则说明程序上一次是非正常退出，因此，需要从 WalF 恢复到内存表中
			loadWal(dir)
		}
	}
}

// 加载文件中的 WalF 到内存
func loadWal(dir string) {
	f, err := os.OpenFile(path.Join(dir, "WalF.log"), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	info, _ := os.Stat(path.Join(dir, "WalF.log"))
	size := info.Size()
	db.WalF = f
	f.Seek(0, 0)
	// 将文件内容全部读取到内存
	data := make([]byte, size)
	f.Read(data)
	// 文件指针移动到最后
	f.Seek(size-1, 0)

	isData := false
	dataLen := 0
	index := int64(0)
	for index <= size {
		if isData {
			tmp := data[index:(index + int64(dataLen))]
			var t kv.Value
			json.Unmarshal(tmp, &t)
			if t.Deleted {
				db.MemoryTree.Delete(t.Key)
			} else {
				db.MemoryTree.Set(t.Key, t.Value)
			}

			isData = false
		} else {
			tmp := data[index:(index + 8)]
			index += 8
			buf := bytes.NewBuffer(tmp)
			binary.Read(buf, binary.LittleEndian, &dataLen)
			isData = true
		}
	}
}
