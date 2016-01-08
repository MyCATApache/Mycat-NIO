package io.mycat.net2;

import java.nio.ByteBuffer;

public class CommonPackageUtil {
	private final static int msyql_packetHeaderSize=4;
	private static final int getPacketLength(ByteBuffer buffer, int offset, int position) {
	    // 处理包头被分割时ByteBuffer越界的情况
        if (offset + msyql_packetHeaderSize >= position) {
            return -1;
        }
		int length = buffer.get(offset) & 0xff;
		length |= (buffer.get(++offset) & 0xff) << 8;
		length |= (buffer.get(++offset) & 0xff) << 16;
		return length + msyql_packetHeaderSize;
	}
	/**
	 * 解析出Package边界,Package为MSQL格式的报文，其他报文可以类比，
	 * @param readBuffer 当前（最后一个bytebuffer）
	 * @param readBufferOffset 上次解析的位置偏移量
	 * @return 下次解析的位置偏移量
	 */
	public static int parsePackages(ByteBufferArray bufferArray, ByteBuffer readBuffer, int readBufferOffset) {
		int offset = readBufferOffset, length = 0, position = readBuffer.position();
		while (offset <= position) {
			int curPacakgeLen = bufferArray.getCurPacageLength();
			if (curPacakgeLen == 0) {// 还没有解析包头获取到长度
				length = getPacketLength(readBuffer, offset, position);
				if (length == -1) {
					// 包头长度不够
					if (!readBuffer.hasRemaining()) {// 没有空间导致没读完包头，当前Buffer直到此结束，包头放到下个bytebufer,防止跨两个bytebufer导致难以处理。
						ByteBuffer newReadBuffer =bufferArray.addNewBuffer();
						// 复制新的包的数据到下一段里
						readBuffer.position(offset);
						newReadBuffer.put(readBuffer);
						// 设置上一个Buffer里的数据到之前的报文结束位置。
						readBuffer.position(offset);
						// 重新从新Buffer的位置0开始
						offset = 0;
						// 返回新的写Buffer
						readBuffer = newReadBuffer;
						break;
					} else {
						// 没有足够数据解析，跳出解析
						break;
					}
				} else {
					// 读取到了包头和长度
					bufferArray.setCurPackageLength(length);

					offset += msyql_packetHeaderSize;
				}

			} else {// 判断当前的数据包是否完整读取
				int totalPackageSize = bufferArray.calcTotalPackageSize();
				int totalLength = bufferArray.getTotalBytesLength();
				int exceededSize = totalLength - totalPackageSize;
				if (exceededSize >= 0) {// 刚好当前报文结束,或者有空间结余
					bufferArray.increatePackageIndex();
					offset = position - exceededSize;
				} else {// 当前数据包还没读完
					offset = 0;
					// 立即返回，否则会将当前ByteBuffer当成新报文去读
					return offset;
				}

			}

		}
		// 返回下一次读取的标记位置
		return offset;
 

	}
}
