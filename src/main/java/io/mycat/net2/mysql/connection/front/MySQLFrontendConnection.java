package io.mycat.net2.mysql.connection.front;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.net2.mysql.connection.MySQLConnection;
import io.mycat.net2.mysql.connection.MySQLConnectionStatus;
import io.mycat.net2.mysql.definination.Capabilities;
import io.mycat.net2.mysql.definination.Versions;
import io.mycat.net2.mysql.packet.ErrorPacket;
import io.mycat.net2.mysql.packet.HandshakePacket;
import io.mycat.net2.mysql.packet.MySQLPacket;
import io.mycat.net2.mysql.util.RandomUtil;

public class MySQLFrontendConnection extends MySQLConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLFrontendConnection.class);

    public static final int LOGIN_STATUS = 1;
    public static final int CMD_RECIEVED_STATUS = 11;
    public static final int RESULT_WAIT_STATUS = 21;
    public static final int RESULT_INIT_STATUS = 22;
    public static final int RESULT_FETCH_STATUS = 23;
    public static final int RESULT_FAIL_STATUS = 29;
    public static final int IDLE_STATUS = -1;
    public static final int QUIT_STATUS = -255;

    public MySQLFrontendConnection(SocketChannel channel) {
        super(channel);
        // TODO Auto-generated constructor stub
        connectedStatus = new MySQLFrontendConnectionStatus();
    }

    @Override
    public String getCharset() {
        // TODO Auto-generated method stub
        return "GBK";
    }

    private class MySQLFrontendConnectionStatus extends MySQLConnectionStatus {

        @Override
        public void setNextStatus(byte packetType) {
            if (packetType == MySQLPacket.COM_QUIT) {
                this.setStatus(QUIT_STATUS);
            }
            int status = this.getStatus();
            switch (status) {
            case LOGIN_STATUS:
                if (packetType == MySQLPacket.AUTH_PACKET) {
                    this.setStatus(IDLE_STATUS);
                }
                break;
            case IDLE_STATUS:
                if (packetType == MySQLPacket.COM_QUERY) {
                    this.setStatus(CMD_RECIEVED_STATUS);
                } else if (packetType == MySQLPacket.COM_QUIT) {
                    this.setStatus(QUIT_STATUS);
                }
                break;
            case CMD_RECIEVED_STATUS:
                if (packetType == MySQLPacket.OK_PACKET) {
                    this.setStatus(RESULT_INIT_STATUS);
                } else if (packetType == MySQLPacket.ERROR_PACKET) {
                    this.setStatus(IDLE_STATUS);
                } else if (packetType == MySQLPacket.COM_QUERY) {
                    this.setStatus(CMD_RECIEVED_STATUS);
                }
                break;
            case RESULT_INIT_STATUS:
                if (packetType == MySQLPacket.OK_PACKET) {
                    this.setStatus(RESULT_FETCH_STATUS);
                } else if (packetType == MySQLPacket.ERROR_PACKET) {
                    this.setStatus(RESULT_FAIL_STATUS);
                }
                break;
            case RESULT_FETCH_STATUS:
                if (packetType == MySQLPacket.EOF_PACKET) {
                    this.setStatus(IDLE_STATUS);
                } else if (packetType == MySQLPacket.ERROR_PACKET) {
                    this.setStatus(RESULT_FAIL_STATUS);
                }
                break;
            case RESULT_FAIL_STATUS:
                if (packetType == MySQLPacket.EOF_PACKET) {
                    this.setStatus(IDLE_STATUS);
                }
                break;
            default:
                LOGGER.warn("Error connected status.", status);
                break;
            }
        }

        @Override
        public int initialValue() {
            return LOGIN_STATUS;
        }

    }

    protected int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // boolean usingCompress = MycatServer.getInstance().getConfig()
        // .getSystem().getUseCompression() == 1;
        // if (usingCompress) {
        // flag |= Capabilities.CLIENT_COMPRESS;
        // }
        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        return flag;
    }

    public void sendAuthPackge() throws IOException {
        // 生成认证数据
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);

        // 保存认证数据
        byte[] seed = new byte[rand1.length + rand2.length];
        System.arraycopy(rand1, 0, seed, 0, rand1.length);
        System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
        this.seed = seed;

        // 发送握手数据包
        HandshakePacket hs = new HandshakePacket();
        hs.packetId = 0;
        hs.protocolVersion = Versions.PROTOCOL_VERSION;
        hs.serverVersion = Versions.SERVER_VERSION;
        hs.threadId = id;
        hs.seed = rand1;
        hs.serverCapabilities = getServerCapabilities();
        // hs.serverCharsetIndex = (byte) (charsetIndex & 0xff);
        hs.serverStatus = 2;
        hs.restOfScrambleBuff = rand2;
        hs.write(this);

        // asynread response
        this.asynRead();
    }

}
