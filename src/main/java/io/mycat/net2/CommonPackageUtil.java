package io.mycat.net2;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonPackageUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(CommonPackageUtil.class);

	private final static int msyql_packetHeaderSize = 4;

	private final static int mysql_packetTypeSize = 1;

	private static final boolean validateHeader(int offset, int position) {
		return offset + msyql_packetHeaderSize + mysql_packetTypeSize < position;
	}

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
	private static final int getPacketType(ByteBuffer buffer, int packetLength, int offset, int position,
			Connection con) {
		if (offset + mysql_packetTypeSize >= position) {
			return MySQLPacket.SPLITTED;
		}
		int type = 0;
		byte field = buffer.get(offset);
		// 目前基本上报文类型可以由data[4]一个字节确定，切mycat-nio中定义的报文类型枚举值与该字节值保持一致
		// 考虑到后面可能会根据报文类型处理部分业务并修改Connection中的状态变量，留出下方各个类型代码入口
		if (con.getDirection() == Connection.Direction.in) {
			// client request
			switch (con.getState()) {
			case connecting:
				// check quit packet
				if (packetLength == MySQLPacket.COM_QUIT_PACKET_LENGTH && field == MySQLPacket.COM_QUIT) {
					type = MySQLPacket.QUIT_PACKET;
				} else {
					type = MySQLPacket.AUTH_PACKET;
					// TODO FAKE
					con.setState(Connection.State.connected);
					// FAKE
				}
				break;
			case connected:
				type = clientBusiness(field, con);
				break;
			default:
				LOGGER.warn("not handled connecton state  err " + con.getState() + " for con " + con);
				break;
			}
		} else if (con.getDirection() == Connection.Direction.out) {
			// mysql response
			switch (con.getState()) {
			case connecting:
				type = mysqlLogin(field, con);
				break;
			case connected:
				type = mysqlBusiness(field, con);
				break;
			default:
				LOGGER.warn("not handled connecton state  err " + con.getState() + " for con " + con);
				break;
			}
		}
		return type;
	}

	private static final int clientBusiness(byte field, Connection con) {
		int type = 0;
		switch (field) {
		case MySQLPacket.COM_INIT_DB:
			type = MySQLPacket.COM_INIT_DB;
			break;
		case MySQLPacket.COM_QUERY:
			type = MySQLPacket.COM_QUERY;
			break;
		case MySQLPacket.COM_PING:
			type = MySQLPacket.COM_PING;
			break;
		case MySQLPacket.COM_QUIT:
			type = MySQLPacket.COM_QUIT;
			break;
		case MySQLPacket.COM_PROCESS_KILL:
			type = MySQLPacket.COM_PROCESS_KILL;
			break;
		case MySQLPacket.COM_STMT_PREPARE:
			type = MySQLPacket.COM_STMT_PREPARE;
			break;
		case MySQLPacket.COM_STMT_EXECUTE:
			type = MySQLPacket.COM_STMT_EXECUTE;
			break;
		case MySQLPacket.COM_STMT_CLOSE:
			type = MySQLPacket.COM_STMT_CLOSE;
			break;
		case MySQLPacket.COM_HEARTBEAT:
			type = MySQLPacket.COM_HEARTBEAT;
			break;
		default:
			// TODO
			break;
		}
		return type;
	}

	private static final int mysqlLogin(byte field, Connection con) {
		int type = 0;
		switch (field) {
		case MySQLPacket.OK_FIELD_COUNT:
			type = MySQLPacket.OK_PACKET;
			break;
		case MySQLPacket.ERROR_FIELD_COUNT:
			type = MySQLPacket.ERROR_PACKET;
			break;
		case MySQLPacket.EOF_FIELD_COUNT:
			type = MySQLPacket.EOF_PACKET;
			break;
		default:
			// TODO
			break;
		}
		return type;
	}

	private static final int mysqlBusiness(byte field, Connection con) {
		int type = 0;
		switch (field) {
		case MySQLPacket.OK_FIELD_COUNT:
			type = MySQLPacket.OK_PACKET;
			break;
		case MySQLPacket.ERROR_FIELD_COUNT:
			type = MySQLPacket.ERROR_PACKET;
			break;
		case MySQLPacket.EOF_FIELD_COUNT:
			type = MySQLPacket.EOF_PACKET;
			break;
		default:
			// TODO
			break;
		}
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
	public static int parsePackages(ByteBufferArray bufferArray, ByteBuffer readBuffer, int readBufferOffset,
			Connection con) {
		int offset = readBufferOffset, length = 0, position = readBuffer.position();
		while (offset <= position) {
			int curPacakgeLen = bufferArray.getCurPacageLength();
			int packetType = bufferArray.getCurPacageType();
			if (curPacakgeLen == 0) {// 还没有解析包头获取到长度
				if (!validateHeader(offset, position)) {
					copyToNewBuffer(bufferArray, readBuffer, offset);
					offset = 0;
					break;
				}
				length = getPacketLength(readBuffer, offset, position);
				// 读取到了包头和长度
				bufferArray.setCurPackageLength(length);
				offset += msyql_packetHeaderSize;
				// 解析报文类型
				packetType = getPacketType(readBuffer, length, offset, position, con);
				bufferArray.setCurPackageType(packetType);
				offset += mysql_packetTypeSize;
			} else {
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
}
