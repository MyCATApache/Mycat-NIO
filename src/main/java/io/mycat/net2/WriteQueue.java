package io.mycat.net2;

import java.util.ArrayList;

/**
 * Connection所要写入的数据队列， 只有reactor线程来负责将此队列中的数据写到socket里，其他任何线程不能写Socket，避免竞争问题
 * 同一时刻只允许一个线程放入数据，通过volatile的标记属性来确保数据在多线程之间的可见性
 * 当队列中的数据超过指定的阀值后，后继的写入则放入磁盘文件中，避免内存过度占用
 * 
 * @author wuzhih
 *
 */
public class WriteQueue {
	// 最大内存长度，单位字节
	private final int maxQueueMemory;
	// 保存Connection的写队列
	private ArrayList<ByteBufferArray> bufferList = new ArrayList<ByteBufferArray>(32);

	// 用来确保bufferList数据写入后的线程可见性
	// 读线程需要先读取此标记，保障写线程写入的数据能被看到
	private volatile int nextWritePos = 0;
	// reactor线程看到的写标志计数器
	private long readerSeenWritPos = -1;

	public WriteQueue(int maxQueueMemory) {
		super();
		this.maxQueueMemory = maxQueueMemory;
	}

	public int size() {
		// todo
		return 0;
	}

	/**
	 * 仅仅供Reactor线程调用，用于写数据到Socket
	 * 
	 * @return ByteBufferArray，如果为空，表示当前没有数据，需要下次调用查看
	 */
	public ByteBufferArray pull() {
		ByteBufferArray ret = this.tryPull();
		if (ret != null) {
			return ret;
		} else {
			// 保障可见性
			long curSeenWritePos = nextWritePos;
			if (readerSeenWritPos != curSeenWritePos) {
				ret = this.tryPull();
				readerSeenWritPos = curSeenWritePos;
			}
		}
		return ret;
	}

	/**
	 * 只能同时一个线程写
	 * 
	 * @param array
	 */
	public void add(ByteBufferArray array) {
		// todo check memory size
		bufferList.add(array);
		nextWritePos++;
	}

	private ByteBufferArray tryPull() {
		if (!bufferList.isEmpty()) {
			return bufferList.remove(0);
		} else {
			return null;
		}
	}

	public void recycle() {
		for (ByteBufferArray arry : this.bufferList) {
			arry.recycle();
		}
		this.bufferList.clear();
	}
}
