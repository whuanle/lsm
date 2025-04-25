package LSM

import (
	"LSM/config"
	"fmt"
	"testing"
	"time"
)

type TestValue struct {
	A int64
	B int64
	C int64
	D string
}

func insert() {

	// 64 个字节
	testV := TestValue{
		A: 1,
		B: 1,
		C: 3,
		D: "00000000000000000000000000000000000000",
	}

	count := 0
	start := time.Now()
	key := []byte{'a', 'a', 'a', 'a', 'a', 'a'}
	Set(string(key), testV)
	for a := 0; a < 1; a++ {
		for b := 0; b < 1; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					for e := 0; e < 26; e++ {
						for f := 0; f < 26; f++ {
							key[0] = 'a' + byte(a)
							key[1] = 'a' + byte(b)
							key[2] = 'a' + byte(c)
							key[3] = 'a' + byte(d)
							key[4] = 'a' + byte(e)
							key[5] = 'a' + byte(f)
							Set(string(key), testV)
							count++
						}
					}
				}
			}
		}
	}
	elapse := time.Since(start)
	fmt.Println("插入完成，数据量：", count, ",消耗时间：", elapse)
}
func Test_Sampletest(t *testing.T) {
	Start(config.Config{
		DataDir:       `SSD`,
		Level0Size:    1,
		PartSize:      4,
		Threshold:     10,
		CheckInterval: 50, // 压缩时间间隔
	})
	// 64 个字节
	testV := TestValue{
		A: 1,
		B: 1,
		C: 3,
		D: "00000000000000000000000000000000000000",
	}
	Set("aaa", testV)
	value, success := Get[TestValue]("aaa")
	if success && value == testV {
		fmt.Println(value)
	} else {
		fmt.Errorf("NotOK")
	}
	Delete[TestValue]("aaa")
}
func Test_Insert(t *testing.T) {
	Start(config.Config{
		DataDir:          `SSD`,
		Level0Size:       1,   // 0 层 SSTable 文件大小
		PartSize:         10,  // 每层文件数量
		Threshold:        500, // 内存表阈值
		CheckInterval:    3,   // 压缩时间间隔
		CompressInterval: 3,
	})
	insert()
	time.Sleep(8 * time.Second)
}
func Test_SampleCompress(t *testing.T) {
	Start(config.Config{
		DataDir:          `SSD`,
		Level0Size:       10, // 0 层 SSTable 文件大小
		PartSize:         4,  // 每层文件数量
		Threshold:        30, // 内存表阈值
		CheckInterval:    50, // 压缩时间间隔
		CompressInterval: 3,
	})
	testV := TestValue{
		A: 1,
		B: 1,
		C: 3,
		D: "00000000000000000000000000000000000000",
	}
	count := 0
	start := time.Now()
	key := []byte{'a', 'a', 'a', 'a'}
	for a := 0; a < 1; a++ {
		for b := 0; b < 1; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					key[0] = 'a' + byte(a)
					key[1] = 'a' + byte(b)
					key[2] = 'a' + byte(c)
					key[3] = 'a' + byte(d)
					Set(string(key), testV)
					count++
				}
			}
		}
	}
	elapse := time.Since(start)
	fmt.Println("插入完成，数据量：", count, ",消耗时间：", elapse)
	time.Sleep(4 * time.Second)
}
func TestInsertSearch(t *testing.T) {
	Start(config.Config{
		DataDir:          `SSD`,
		Level0Size:       10, // 0 层 SSTable 文件大小
		PartSize:         4,  // 每层文件数量
		Threshold:        30, // 内存表阈值
		CheckInterval:    3,  // 压缩时间间隔
		CompressInterval: 3,
	})
	insert()
	testV := TestValue{
		A: 1,
		B: 1,
		C: 3,
		D: "00000000000000000000000000000000000000",
	}

	count := 0
	key := []byte{'a', 'a', 'a', 'a', 'a', 'a'}
	//Set(string(key), testV)
	for a := 0; a < 1; a++ {
		for b := 0; b < 1; b++ {
			for c := 0; c < 26; c++ {
				for d := 0; d < 26; d++ {
					for e := 0; e < 26; e++ {
						for f := 0; f < 26; f++ {
							key[0] = 'a' + byte(a)
							key[1] = 'a' + byte(b)
							key[2] = 'a' + byte(c)
							key[3] = 'a' + byte(d)
							key[4] = 'a' + byte(e)
							key[5] = 'a' + byte(f)
							v, _ := Get[TestValue](string(key))
							if v != testV {
								fmt.Errorf("NotEqual")
							} else {
								fmt.Println("SearchCorrect  ", key)
							}
							count++
						}
					}
				}
			}
		}
	}

}
