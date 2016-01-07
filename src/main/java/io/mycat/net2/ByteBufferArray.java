package io.mycat.net2;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 * ByteBuffer数组，扩展的时候从BufferPool里动态获取可用的Buffer 非线程安全类
 * 
 * @author wuzhih
 *
 */
public class ByteBufferArray {
	private final ReactorBufferPool bufferPool;

	private final ArrayList<ByteBuffer> writedBlockLst = new ArrayList<ByteBuffer>(4);

	public ByteBufferArray(ReactorBufferPool bufferPool) {
		super();
		this.bufferPool = bufferPool;

	}

	public int getBlockCount() {
		return writedBlockLst.size() + 1;
	}

	/**
	 * 得到队列中最后一个ByteBuffer，也即当前可以写入的位置，如果队列为空，则返回NULL
	 * 
	 * @return
	 */
	public ByteBuffer getLastByteBuffer() {
		int size = writedBlockLst.size();
		return (size == 0) ? null : writedBlockLst.get(writedBlockLst.size() - 1);
	}

	/**
	 * 申请一个ByteBuffer，并且放入队列，并且返回此ByteBuffer
	 * 
	 * @return
	 */
	public ByteBuffer addNewBuffer() {
		ByteBuffer buf = this.bufferPool.allocateByteBuffer();
		writedBlockLst.add(buf);
		return buf;
	}

	public ArrayList<ByteBuffer> getWritedBlockLst() {
		return writedBlockLst;
	}

	/**
	 * 将一个数组写入队列中
	 * 
	 * @param src
	 */
	public void write(byte[] src) {
		ByteBuffer curWritingBlock = getLastByteBuffer();
		if (curWritingBlock == null) {
			curWritingBlock = addNewBuffer();
		}
		int offset = 0;
		int remains = src.length;
		while (remains > 0) {
			int writeable = curWritingBlock.remaining();
			if (writeable >= remains) {
				// can write whole srce
				curWritingBlock.put(src, offset, remains);
				break;
			} else {
				// can write partly
				curWritingBlock.put(src, offset, writeable);
				offset += writeable;
				remains -= writeable;
				curWritingBlock = addNewBuffer();
				continue;
			}

		}
	}

	/**
	 * 回收此对象，用完需要在合适的地方释放，否則產生內存泄露問題
	 */
	public void recycle() {
		bufferPool.recycle(this);
	}

	protected void clear() {

		this.writedBlockLst.clear();

	}

}
