
## 使用方法

下载依赖包：
```go
go get -u github.com/whuanle/lsm@v1.0.0
```
配置启动程序：
```go
import (
"github.com/whuanle/lsm"
"github.com/whuanle/lsm/config"
)

	lsm.Start(config.Config{
		DataDir:    `E:\项目\lsm数据测试目录`,
		Level0Size: 10,
		PartSize:   4,
		Threshold:  1000,
		CheckInterval: 3, // 压缩时间间隔
	})
	
```
Level0Size：第 0 层的 SSTable 表总大小超过这样阈值时，进行文件合并；   
PartSize：每层 SSTable 文件数量超过这个值，进行文件合并；  
Threshold：内存表元素数量阈值，超过这个值，将会被压缩到 SSTable；   
CheckInetrval：后台独立线程指向间隔时间，独立线程会检查内存表和所有层的 SSTable ，确定是否需要执行压缩；  


完整增删查改代码如下：
```go
package main

import (
	"fmt"
	"github.com/whuanle/lsm"
	"github.com/whuanle/lsm/config"
)

type TestValue struct {
	A int64
	B int64
	C int64
	D string
}

func main() {
	lsm.Start(config.Config{
		DataDir:    `E:\项目\lsm数据测试目录`,
		Level0Size: 1,
		PartSize:   4,
		Threshold:  500,
	})
	// 64 个字节
	testV := TestValue{
		A: 1,
		B: 1,
		C: 3,
		D: "00000000000000000000000000000000000000",
	}

	lsm.Set("aaa", testV)

	value, success := lsm.Get[TestValue]("aaa")
	if success {
		fmt.Println(value)
	}

	lsm.Delete("aaa")
}
```