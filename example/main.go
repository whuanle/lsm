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

func query() {
	start := time.Now()
	v, _ := lsm.Get[TestValue]("aaaaaa")
	elapse := time.Since(start)
	fmt.Println("查找 aaaaaa 完成，消耗时间：", elapse)
	fmt.Println(v)

	start = time.Now()
	v, _ = lsm.Get[TestValue]("aazzzz")
	elapse = time.Since(start)
	fmt.Println("查找 aazzzz 完成，消耗时间：", elapse)
	fmt.Println(v)
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
