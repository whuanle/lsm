
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
	})
	
```