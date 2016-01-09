package io.mycat.net2.mysql;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import io.mycat.net2.ByteBufferArray;
import io.mycat.net2.Connection;

public abstract class MySQLConnection extends Connection {

    protected MySQLConnectionStatus connectedStatus;

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
    protected void parseProtocolPakage(ByteBufferArray readBufferArray, ByteBuffer readBuffer, int readBufferOffset) {
        CommonPackageUtil.parsePackages(readBufferArray, readBuffer, readBufferOffset, this);
    }
}
