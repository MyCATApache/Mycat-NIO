package io.mycat.net2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * ByteBuffer数组，扩展的时候从BufferPool里动态获取可用的Buffer 非线程安全类
 * 
 * 
 * @author wuzhih
 *
 */
public class ByteBufferArray {

	/** packageLengths数组中表示报文类型的偏移量 */
	private static final int PACKAGE_TYPE_SHIFT = 24;
	
	/** packageLengths数组中表示包长度低 24位 */
	private static final int PACKAGE_LENGTH_UNIT = (1 << PACKAGE_TYPE_SHIFT) - 1;
	
	/** packageLengths数组容量 */
	private static final int CAPACITY = 4;

	private final ReactorBufferPool bufferPool;

	private final ArrayList<ByteBuffer> writedBlockLst = new ArrayList<ByteBuffer>(4);
	// 此Array中包括的消息报文长度（byte 字节的长度而不是writedBlockLst中的次序）
	private int[] packageLengths = new int[CAPACITY];
	private int curPacageIndex = 0;

	public ByteBufferArray(ReactorBufferPool bufferPool) {
		super();
		this.bufferPool = bufferPool;

	}

	public int getCurPacageIndex() {
		return curPacageIndex;
	}

	/**
	 * 当前线程是否是此对象所属的Reactor线程
	 * 
	 * @return
	 */
	public boolean iscurReactorThread() {
		return (Thread.currentThread() == bufferPool.getReactorThread());
	}

	/**
	 * 获取当前ByteBuffer Posion位置相对应的绝对位置，比如此ByteBuffer之前有2个ByteBuffer，则绝对位置为
	 * 第一个的长度+第二个的长度+本身的postion
	 * 
	 * @return
	 */
	public int getAbsByteBufPosion(ByteBuffer theButBuf) {
		int absPos = 0;
		int endBlock = writedBlockLst.size() - 1;
		for (int i = 0; i < endBlock; i++) {
			ByteBuffer bytBuf = writedBlockLst.get(i);
			if (bytBuf != theButBuf) {
				absPos += bytBuf.position();
			}
		}
		return absPos + theButBuf.position();

	}

	/**
	 * 返回当前所有bytebuffer里的字节数长度总和
	 * 
	 * @return
	 */
	public int getTotalBytesLength() {
		int totalLen = 0;
		int endBlock = writedBlockLst.size();
		for (int i = 0; i < endBlock; i++) {
			ByteBuffer bytBuf = writedBlockLst.get(i);
			totalLen += bytBuf.position();
		}
		return totalLen;
	}

	/**
	 * 计算所有packages的字节总数
	 * 
	 * @return
	 */
	public int calcTotalPackageSize() {
		int totalBytes = 0;
		for (int i = 0; i < this.curPacageIndex + 1; i++) {
			totalBytes += packageLengths[i] & PACKAGE_LENGTH_UNIT;
		}
		return totalBytes;
	}

	/**
	 * 得到队列中最后一个ByteBuffer，也即当前可以写入的位置，如果队列为空，则返回NULL
	 * 
	 * @return
	 */
	public ByteBuffer getLastByteBuffer() {
		return writedBlockLst.get(writedBlockLst.size() - 1);

	}

	public int getBlockCount() {
		return writedBlockLst.size();
	}

	public ByteBuffer getBlock(int i) {
		return writedBlockLst.get(i);
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
	 * 设置package
	 * 
	 * @param packegeNum
	 * @param startIndex
	 */
	public void setCurPackageLength(int packageLenth) {
		this.packageLengths[curPacageIndex] = (getCurPacageType() << PACKAGE_TYPE_SHIFT) | packageLenth;
	}
	
	public void setCurPackageType(int packageType){
		this.packageLengths[curPacageIndex] = (packageType << PACKAGE_TYPE_SHIFT) | getCurPacageLength();
	}

	/**
	 * 将一个数组写入队列中
	 * 
	 * @param src
	 */
	public void write(byte[] src) {
		ByteBuffer curWritingBlock = null;
		if (this.writedBlockLst.isEmpty()) {
			curWritingBlock = this.addNewBuffer();
		} else {
			curWritingBlock = getLastByteBuffer();
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

	public int getCurPacageLength() {
		return this.packageLengths[this.curPacageIndex] & PACKAGE_LENGTH_UNIT;
	}
	
	public byte getCurPacageType() {
		return getPacageType(this.curPacageIndex);
	}
	
	public byte getPacageType(int index){
		return (byte)(this.packageLengths[index] >>> PACKAGE_TYPE_SHIFT);
	}
	
	/**
	 * 回收此对象，用完需要在合适的地方释放，否則產生內存泄露問題
	 */
	public void recycle() {
		bufferPool.recycle(this);
	}

	protected void clear() {
		curPacageIndex = 0;
		this.writedBlockLst.clear();
		for (int i = 0; i < packageLengths.length; i++) {
			packageLengths[i] = 0;
		}

	}

	public void increatePackageIndex() {
		// 超过预期报文数量，将数组容量扩展
		if (++curPacageIndex >= CAPACITY) {
			packageLengths = Arrays.copyOf(packageLengths, packageLengths.length + CAPACITY);
		}
	}

}
