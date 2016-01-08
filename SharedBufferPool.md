# [SharedBufferPool](https://github.com/MyCATApache/Mycat-NIO/blob/master/src/main/java/io/mycat/net2/SharedBufferPool.java)

这是Mycat-NIO框架的ByteBuffer线程共享调度池，用于在各个Reactor线程间分配新的ByteBuffer空间或从共享池中调度空闲ByteBuffer。

## [TestReactorBufferPool.testMutilTreadLocateAndFree](https://github.com/MyCATApache/Mycat-NIO/blob/master/src/main/test/io/mycat/net2/TestReactorBufferPool.java)

这个方法启动了10个Reactor线程，绑定在同一个SharedBufferPool上。每个线程不断的重复如下动作。
```Java
分配100个ByteBuffer
写入数据
批量回收100个ByteBuffer
```
每次Reactor线程在分配ByteBuffer时，会优先在自己维护的空闲池里寻找是否有空闲空间，没有则调用绑定的SharedBufferPool分配空间。
在SharedBufferPool分配新空间时，有一个全局计数器newCreated
```Java
private **volatile** int newCreated;

public ByteBuffer allocate() {
	ByteBuffer node = freeBuffers.poll();
	if (node == null) {
		**newCreated++;**
		node = this.createDirectBuffer(chunkSize);
	} else {
		node.clear();
	}
	return node;
}
```
在TestReactorBufferPool.testMutilTreadLocateAndFree方法中，主线程等待所有reactor线程完成后会统计newCreated值。预期结果应该是10*100=1000，但是执行结果却一直在900-1000浮动，基本没有到1000的情况。

## Volatile与原子性操作

在jsr-133增强volatile的语义后，JMM提供如下两种原子性的volatile操作。
* volatile读
	强制让工作线程的缓存行失效，从主存中读取最新的变量值。
* volatile写
	写入的新值立即同步到主存（缓存一致性协议）

上述两种操作可以保证，在对一个volatile变量进行原子性读写操作时，其他线程立即对新值可见。
但是，针对i++这种操作，看似是一条原子性指令，实际上编译后会被翻译为多条组合指令。i++包含3个操作：
```Java
从工作内存中读取i的值并入栈
栈顶+1
栈顶写入i的工作内存。
```

回到SharedBufferPool.allocate方法，由于操作的是volatile变量，所以newCreated++的操作为：
```Java
从主存中读取newCreated的值并入栈
栈顶+1
栈顶写入newCreated的主存。
```
volatile只能保证原子性读写操作对其他线程的可见性，那么在10个reactor线程中，可能出现如下指令时序
```Java
Thread1					Thread2
读取newCreated(0)		
						读取newCreated(0)
栈顶+1(1)
						栈顶+1(1)
1写入newCreated的主存
						1写入newCreated的主存。
```
如此，两个线程分别执行了一次allocate方法后，newCreated的当前值为1，但是分配了2块ByteBuffer内存。

## 保证++操作的原子性与CAS
JUC中提供了一系列原子类型工具。java.util.concurrent.atomic.AtomicInteger是原子性操作整型变量的工具，其中可以除保证原子性读写外，还提供了如addAndGet、incrementAndGet等原子性计算方法。AtomicInteger使用循环CAS的无锁方式实现同步，避免了临界区的调度与线程状态的切换。

为了验证CAS的效果，我们替换了newCreated的类型并修改了allocate的实现
```Java
private final **AtomicInteger** newCreated;

public ByteBuffer allocate() {
	ByteBuffer node = freeBuffers.poll();
	if (node == null) {
		**newCreated.getAndIncrement();**
		node = this.createDirectBuffer(chunkSize);
	} else {
		node.clear();
	}
	return node;
}
```
重新执行TestReactorBufferPool.testMutilTreadLocateAndFree方法，发现newCreated的值稳定在了1000，与预期结果一致。

## 结论
虽然AtomicInteger可以保证原子性++操作，allocate可以统计出正确的新创建ByteBuffer的数量，但是循环CAS在并发较高的情况下会空耗CPU导致性能下降。
所以目前系统采用折中的方法，由于这个统计不需要严格精确，使用volatile实现变量的弱一致性，只统计大致的结果。这样可以保证Mycat-NIO框架的整体性能。

BTW：后期可能会替换为非volatile的普通变量。