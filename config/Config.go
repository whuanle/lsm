package config

import "sync"

// Config 数据库启动配置
type Config struct {
	// 数据目录
	DataDir string
	// 0 层的 所有 SsTable 文件大小总和的最大值，单位 MB，超过此值，该层 SsTable 将会被压缩到下一层
	Level0Size int
	// 每层中 SsTable 表数量的阈值，该层 SsTable 将会被压缩到下一层
	PartSize int
	// 内存表的 kv 最大数量，超出这个阈值，内存表将会被保存到 SsTable 中
	Threshold int
	// 检查内存树大小的时间间隔，多久进行一次检查，如果超出就放入iMemTable中
	CheckInterval int
	// 压缩内存的时间间隔，多久进行一次检查iMemTable不为空的压缩工作
	CompressInterval int
}

var once *sync.Once = &sync.Once{}

// 常驻内存
var config Config

// Init 初始化数据库配置
func Init(con Config) {
	once.Do(func() {
		config = con
	})
}

// GetConfig 获取数据库配置
func GetConfig() Config {
	return config
}
