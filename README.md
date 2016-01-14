# Mycat-NIO
非常高性能又简单的Mycat NIO框架，被很多人用于RPC开发以及基础平台中

## 关于[SharedBufferPool](https://github.com/MyCATApache/Mycat-NIO/blob/master/src/main/java/io/mycat/net2/SharedBufferPool.java)的newCreated计数
```java
private volatile int newCreated;
```
目前使用volatile变量统计新分配的ByteBuffer。这个统计不需要严格精确，但相较于AtomicInteger的CAS实现，volatile保证了NIO框架的整体性能。

[[更多信息]](https://github.com/MyCATApache/Mycat-NIO/blob/master/SharedBufferPool.md)


## [关于MockMySQLServer](https://github.com/MyCATApache/Mycat-NIO/blob/master/mock_mysql_server.md)