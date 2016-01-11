package io.mycat.net2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * 此BufferPool主要是为单一线程使用，即Reactor线程专用的BufferPool，因此读取数据不用加锁，
 * 如果是其他线程来获取或归还数据，则用Volatile的机制和延迟技术以获取最高的性能
 * 
 * @author wuzhih
 *
 */
public class ReactorBufferPool {
	// 属于哪个reactor thread
	private final Thread reactorThread;
	// Reactor线程归还的Buffer
	private final LinkedList<ByteBuffer> freeBuffers = new LinkedList<ByteBuffer>();
	// 共享ByteBuffer池，真实分配ByteBuffer的地方
	private final SharedBufferPool sharedBufferPool;
	// 最多存放多少个byteBuffer，超过部分则放入共享池
	private final int maxFreeCount;
	// 池化的ByteBufferArray对象，为了降低创建的频率
	private final LinkedList<ByteBufferArray> extByteBufferPool = new LinkedList<ByteBufferArray>();

	public ReactorBufferPool(SharedBufferPool shearedBufferPool, Thread reactorThread, int maxFreeCount) {
		this.sharedBufferPool = shearedBufferPool;
		this.reactorThread = reactorThread;
		this.maxFreeCount = maxFreeCount;

	}

	public int getCurByteBuffersCount() {
		return freeBuffers.size();
	}

	public Thread getReactorThread() {
		return reactorThread;
	}

	/**
	 * 分配一个ByteBufferArray,ByteBufferArray使用完成后需要回收
	 * 
	 * @return ByteBufferArray
	 */
	public ByteBufferArray allocate() {
		if (Thread.currentThread() == reactorThread) {
			if (!extByteBufferPool.isEmpty()) {
				ByteBufferArray result = extByteBufferPool.removeLast();
				result.clear();
				return result;
			}
		}
		return new ByteBufferArray(this);
	}

	/**
	 * 回收ByteBufferArrayd
	 * 
	 * @param ByteBufferArray
	 */
	public void recycle(ByteBufferArray extBuffer) {
		if (Thread.currentThread() == reactorThread) {
			extByteBufferPool.add(extBuffer);
			// reactor线程回收
			if (freeBuffers.size() < maxFreeCount) {
				ArrayList<ByteBuffer> arrayList = extBuffer.getWritedBlockLst();
				long size = arrayList.size();
				for (int i = 0; i < size; i++) {
					freeBuffers.add(arrayList.get(i));
				}
				return;
			}
		}

		// 共享池回收
		sharedBufferPool.recycle(extBuffer.getWritedBlockLst());

	}

	protected ByteBuffer allocateByteBuffer() {
		if (Thread.currentThread() == reactorThread) {
			if (!freeBuffers.isEmpty()) {
				ByteBuffer buf = freeBuffers.removeLast();
				buf.clear();
				return buf;
			}
		}
		// 另外线程要求分配或者当前用完了，从共享BufferPool获取
		return this.sharedBufferPool.allocate();
	}

    public SharedBufferPool getSharedBufferPool() {
        return sharedBufferPool;
    }

}
