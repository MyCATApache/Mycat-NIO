package io.mycat.net2.mysql.connection.back;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.net2.ByteBufferArray;
import io.mycat.net2.Connection;
import io.mycat.net2.ConnectionException;
import io.mycat.net2.NIOHandler;
import io.mycat.net2.mysql.definination.Capabilities;
import io.mycat.net2.mysql.packet.EOFPacket;
import io.mycat.net2.mysql.packet.ErrorPacket;
import io.mycat.net2.mysql.packet.HandshakePacket;
import io.mycat.net2.mysql.packet.MySQLPacket;
import io.mycat.net2.mysql.packet.OkPacket;
import io.mycat.net2.mysql.packet.Reply323Packet;
import io.mycat.net2.mysql.packet.util.CharsetUtil;
import io.mycat.net2.mysql.packet.util.SecurityUtil;

public class MySQLBackendConnectionHandler implements NIOHandler<MySQLBackendConnection> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackendConnectionHandler.class);

    private static final int RESULT_STATUS_INIT = 0;
    private static final int RESULT_STATUS_HEADER = 1;
    private static final int RESULT_STATUS_FIELD_EOF = 2;

    @Override
    public void onConnected(MySQLBackendConnection con) throws IOException {

        // con.asynRead();
    }

    @Override
    public void handle(MySQLBackendConnection con, ByteBufferArray bufferArray) {

        int connectedStatus = con.getConnectedStatus();
        int packetIndex = bufferArray.getCurPacageIndex() - 1;
        switch (con.getState()) {
        case connecting: {
            LOGGER.debug("backend handle login", con);
            doConnecting(con, bufferArray, packetIndex);
            break;
        }
        case connected: {
            try {
                LOGGER.debug("backend handle business", con);
                doHandleBusinessMsg(con, bufferArray, packetIndex);
            } catch (Exception e) {
                LOGGER.warn("caught err of con " + con, e);
            }
            break;
        }

        default:
            LOGGER.warn("not handled connecton state  err " + con.getState() + " for con " + con);
            break;

        }
        bufferArray.setCurHandlingPacageIndex(bufferArray.getCurPacageIndex());

    }

    @Override
    public void onConnectFailed(MySQLBackendConnection source, Throwable e) {

    }

    private void handleLogin(MySQLBackendConnection source, ByteBufferArray bufferArray, int packetIndex) {
        try {
            switch (bufferArray.readPacket(packetIndex, 4)) {
            case OkPacket.FIELD_COUNT:
                HandshakePacket packet = source.getHandshake();
                if (packet == null) {
                    processHandShakePacket(source, bufferArray, packetIndex);
                    // 发送认证数据包
                    source.authenticate();
                    break;
                }
                // 处理认证结果
                source.setAuthenticated(true);
                source.setConnectedStatus(MySQLBackendConnection.IDLE_STATUS);
                source.setState(Connection.State.connected);
                boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS
                        & packet.serverCapabilities);
                boolean usingCompress = false;

                if (clientCompress && usingCompress) {
                    // source.setSupportCompress(true);
                }

                // if (source.getRespHandler() != null) {
                // source.getRespHandler().connectionAcquired(source);
                // }

                break;
            case ErrorPacket.FIELD_COUNT:
                ErrorPacket err = new ErrorPacket();
                err.read(bufferArray, packetIndex);
                String errMsg = new String(err.message);
                LOGGER.warn("can't connect to mysql server ,errmsg:" + errMsg + " " + source);
                // source.close(errMsg);
                throw new ConnectionException(err.errno, errMsg);

            case EOFPacket.FIELD_COUNT:
                auth323(source, bufferArray.readPacket(packetIndex, 3));
                break;
            default:
                packet = source.getHandshake();
                if (packet == null) {
                    processHandShakePacket(source, bufferArray, packetIndex);
                    // 发送认证数据包
                    source.authenticate();
                    break;
                } else {
                    throw new RuntimeException("Unknown Packet!");
                }

            }

        } catch (RuntimeException e) {
            throw e;
        }
    }

    @Override
    public void onClosed(MySQLBackendConnection source, String reason) {

    }

    private void doConnecting(MySQLBackendConnection con, ByteBufferArray bufferArray, int packetIndex) {
        handleLogin(con, bufferArray, packetIndex);
    }

    public void doHandleBusinessMsg(final MySQLBackendConnection source, final ByteBufferArray bufferArray,
            final int packetIndex) {
        handleData(source, bufferArray, packetIndex);
    }

    public void connectionError(Throwable e) {

    }

    protected void handleData(final MySQLBackendConnection source, final ByteBufferArray bufferArray,
            final int packetIndex) {
        LOGGER.debug("handle data business.");
        int lastPacketIndex = bufferArray.getCurPacageIndex() - 1;
        // TODO MOCK
        source.getResponseHandler().handleResponse(bufferArray, source);

        byte type = bufferArray.getPacageType(lastPacketIndex);
        if (source.getConnectedStatus() == MySQLBackendConnection.IDLE_STATUS && (type == MySQLPacket.EOF_PACKET
                || type == MySQLPacket.ERROR_PACKET || type == MySQLPacket.OK_PACKET)) {
            source.getResponseHandler().finishResponse(source);
        }
    }

    private void processHandShakePacket(MySQLBackendConnection source, final ByteBufferArray bufferArray,
            final int packetIndex) {
        // 设置握手数据包
        HandshakePacket packet = new HandshakePacket();
        packet.read(bufferArray, packetIndex);
        source.setHandshake(packet);
        // source.setThreadId(packet.threadId);

        // 设置字符集编码
        int charsetIndex = (packet.serverCharsetIndex & 0xff);
        String charset = CharsetUtil.getCharset(charsetIndex);
        // if (charset != null) {
        // source.setCharset(charset);
        // } else {
        // LOGGER.warn("Unknown charsetIndex:" + charsetIndex);
        // throw new RuntimeException("Unknown charsetIndex:" + charsetIndex);
        // }
    }

    private void auth323(MySQLBackendConnection source, byte packetId) {
        // 发送323响应认证数据包
        Reply323Packet r323 = new Reply323Packet();
        r323.packetId = ++packetId;
        String pass = source.getPassword();
        if (pass != null && pass.length() > 0) {
            byte[] seed = source.getHandshake().seed;
            r323.seed = SecurityUtil.scramble323(pass, new String(seed)).getBytes();
        }
        r323.write(source);
    }

}
