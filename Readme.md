# lsm
主要代码来自于[whuanle/lsm](https://github.com/whuanle/lsm)，
为了方便应用到[huiming23344/kv-raft](https://github.com/huiming23344/kv-raft)中，在原作者基础上进行了一些修改和完善。

## 主要改动
加入`iMemTable`结构，解决了Compress时WalF会被误删的问题

## 使用方法

下载依赖包：
```go
go get -u github.com/huiming/lsm@v1.0.0
```



## 参考

- [lsm on github by whuanle](https://github.com/whuanle/lsm)
- [痴者工良的cnblogs](https://www.cnblogs.com/whuanle/p/16297025.html)