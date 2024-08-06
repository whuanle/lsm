package main

import (
	"bufio"
	"fmt"
	"github.com/whuanle/lsm"
	"github.com/whuanle/lsm/config"
	"os"
	"time"
)

type TestValue struct {
	A int64
	B int64
	C int64
	D string
}

func main() {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r)
			inputReader := bufio.NewReader(os.Stdin)
			_, _ = inputReader.ReadString('\n')
		}
	}()
	lsm.Start(config.Config{
		DataDir:       `E:\项目\lsm数据测试目录`,
		Level0Size:    100,
		PartSize:      4,
		Threshold:     3000,
		CheckInterval: 3,
	})
	query()

}

func testResetWalBug() {
	defer func() {
		r := recover()
		if r != nil {
			fmt.Println(r)
			inputReader := bufio.NewReader(os.Stdin)
			_, _ = inputReader.ReadString('\n')
		}
	}()
	lsm.Start(config.Config{
		DataDir:          "./data",
		Level0Size:       100,
		PartSize:         4,
		Threshold:        9,
		CheckInterval:    2,
		CompressInterval: 2,
	})
	//注释掉以下五行代码，再次运行以检验wal中新数据是否被删除
	//insert 10 to start compress
	insertValuesByCount(10, 10)
	// wait for compress and insert kvs when compress begins
	time.Sleep(2 * time.Second)
	insertValuesByCount(6, 0)
	keys := []string{"0", "1", "2", "3", "4", "5"}
	queryByKeys(keys)
	time.Sleep(10 * time.Second)
}

func queryByKeys(keys []string) {
	for _, key := range keys {
		start := time.Now()
		v, _ := lsm.Get[TestValue](key)
		elapse := time.Since(start)
		fmt.Println("查找", key, " 完成，消耗时间：", elapse)
		fmt.Println(v)
	}
}

func query() {
	start := time.Now()
	v, _ := lsm.Get[TestValue]("4")
	elapse := time.Since(start)
	fmt.Println("查找 aaaaaa 完成，消耗时间：", elapse)
	fmt.Println(v)

	start = time.Now()
	v, _ = lsm.Get[TestValue]("2")
	elapse = time.Since(start)
	fmt.Println("查找 aazzzz 完成，消耗时间：", elapse)
	fmt.Println(v)
}

func insertValuesByCount(count, startFrom int) {
	start := time.Now()
	// 64 个字节
	testV := TestValue{
		A: 1,
		B: 1,
		C: 3,
		D: "00000000000000000000000000000000000000",
	}
	for i := 0; i < count; i++ {
		lsm.Set(fmt.Sprint(i+startFrom), testV)
	}
	elapse := time.Since(start)
	fmt.Println("插入完成，数据量：", count, ",消耗时间：", elapse)
}

func insert() {
	// 64 个字节
	testV := TestValue{
		A: 1,
		B: 1,
		C: 3,
		D: "00000000000000000000000000000000000000",
	}

	//testVData, _ := json.Marshal(testV)
	//// 131 个字节
	//kvData, _ := kv.Encode(kv.Value{
	//	Key:     "abcdef",
	//	Value:   testVData,
	//	Deleted: false,
	//})
	//fmt.Println(len(kvData))
	//position := ssTable.Position{}
	//// 35 个字节
	//positionData, _ := json.Marshal(position)
	//fmt.Println(len(positionData))
	//
	count := 0
	start := time.Now()
	key := []byte{'a', 'a', 'a', 'a', 'a', 'a'}
	lsm.Set(string(key), testV)
	for a := 0; a < 26; a++ {
		for b := 0; b < 26; b++ {
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
							lsm.Set(string(key), testV)
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
