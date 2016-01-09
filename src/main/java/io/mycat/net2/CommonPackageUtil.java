package io.mycat.net2;

import java.nio.ByteBuffer;

public class CommonPackageUtil {
	private final static int msyql_packetHeaderSize = 4;

	private final static int mysql_packetTypeSize = 8;

	/**
	 * 获取报文长度
	 * 
	 * @param buffer
	 *			报文buffer
	 * @param offset
	 *			buffer解析位置偏移量
	 * @param position
	 *			buffer已读位置偏移量
	 * @return 报文长度(Header长度+内容长度)
	 */
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
	 * 获取报文类型(0-255)
	 * 
	 * @param buffer
	 *			报文buffer
	 * @param offset
	 *			buffer解析位置偏移量
	 * @param position
	 *			buffer已读位置偏移量
	 * @return 报文类型标识
	 */
	private static final int getPacketType(ByteBuffer buffer, int offset, int position) {
		if (offset + mysql_packetTypeSize >= position) {
			return -1;
		}
		// Fake实现，后面应改成正确的解析
		int type = 0;
		for (int i = 0; i < mysql_packetTypeSize; i++) {
			type += buffer.get(offset + i);
		}
		// Fake end
		return type;
	}

	/**
	 * 将当前解析bytebuffer被截断的部分复制到新的buffer里，新buffer作为bufferArray的队列最新位置
	 * 
	 * @param readBuffer
	 *			当前（最后一个bytebuffer）
	 * @param offset
	 *			上次解析的位置偏移量
	 */
	private static void copyToNewBuffer(ByteBufferArray bufferArray, ByteBuffer readBuffer, int offset) {
		ByteBuffer newReadBuffer = bufferArray.addNewBuffer();
		// 复制新的包的数据到下一段里
		readBuffer.position(offset);
		newReadBuffer.put(readBuffer);
		// 为了计算TotalBytesLength，设置当前buffer终点
		readBuffer.position(offset);
	}

	/**
	 * 解析出Package边界,Package为MSQL格式的报文，其他报文可以类比，
	 * 
	 * @param readBuffer
	 *			当前（最后一个bytebuffer）
	 * @param readBufferOffset
	 *			上次解析的位置偏移量
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
						copyToNewBuffer(bufferArray, readBuffer, offset);
						// 重新从新Buffer的位置0开始
						offset = 0;
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

			} else {
				// 判断报文类型是否已解析
				int packetType = bufferArray.getCurPacageType();
				if (packetType == PacketType.UNREAD) {
					packetType = getPacketType(readBuffer, offset, position);
					if (packetType == -1) {
						// 类型被截断
						if (offset < position) {
							// 如果类型有至少1个字节被截断，则复制到新buffer
							copyToNewBuffer(bufferArray, readBuffer, offset);
						}
						offset = 0;
						break;
					} else if (packetType < 0 || packetType > 255) {
						// 类型错误
						break;
					} else {
						bufferArray.setCurPackageType(packetType);
						offset += mysql_packetTypeSize;
					}
				}

				// 判断当前的数据包是否完整读取
				int totalPackageSize = bufferArray.calcTotalPackageSize();
				int totalLength = bufferArray.getTotalBytesLength();
				int exceededSize = totalLength - totalPackageSize;
				if (exceededSize >= 0) {// 刚好当前报文结束,或者有空间结余
					bufferArray.increatePackageIndex();
					offset = position - exceededSize;
				} else {// 当前数据包还没读完
					offset = 0;
					// 立即返回，否则会将当前ByteBuffer当成新报文去读
					break;
				}

			}

		}
		// 返回下一次读取的标记位置
		return offset;

	}

	/**
	 * 报文类型枚举
	 */
	static interface PacketType {
		/** 未解析报文 */
		int UNREAD = 0;
	}
}
