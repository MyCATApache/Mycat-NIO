package io.mycat.net2.mysql.connection;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import io.mycat.net2.ByteBufferArray;
import io.mycat.net2.Connection;
import io.mycat.net2.mysql.packet.ErrorPacket;
import io.mycat.net2.mysql.packet.util.CommonPacketUtil;

public abstract class MySQLConnection extends Connection {

    protected MySQLConnectionStatus connectedStatus;

    protected byte[] seed;

    public MySQLConnection(SocketChannel channel) {
        super(channel);
        // TODO Auto-generated constructor stub
    }

    public int getConnectedStatus() {
        return connectedStatus.getStatus();
    }

    public void setConnectedStatus(int connectedStatus) {
        this.connectedStatus.setStatus(connectedStatus);
    }

    public void setNextConnectedStatus(byte packetType) {
        this.connectedStatus.setNextStatus(packetType);
    }

    @Override
    protected int parseProtocolPakage(ByteBufferArray readBufferArray, ByteBuffer readBuffer, int readBufferOffset) {
        return CommonPacketUtil.parsePackets(readBufferArray, readBuffer, readBufferOffset, this);
    }
    
    public void writeErrMessage(int errno, String info) {
        ErrorPacket err = new ErrorPacket();
        err.packetId = 1;
        err.errno = errno;
        err.message = info.getBytes();
        err.write(this);
    }
}
