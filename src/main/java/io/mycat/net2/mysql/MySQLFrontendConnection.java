package io.mycat.net2.mysql;

import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLFrontendConnection extends MySQLConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLFrontendConnection.class);

    public static final int LOGIN_STATUS = 1;
    public static final int CMD_RECIEVED_STATUS = 11;
    public static final int RESULT_WAIT_STATUS = 21;
    public static final int RESULT_INIT_STATUS = 22;
    public static final int RESULT_FETCH_STATUS = 23;
    public static final int RESULT_FAIL_STATUS = 29;
    public static final int IDLE_STATUS = -1;

    public MySQLFrontendConnection(SocketChannel channel) {
        super(channel);
        // TODO Auto-generated constructor stub
        connectedStatus = new MySQLFrontendConnectionStatus();
    }

    @Override
    public String getCharset() {
        // TODO Auto-generated method stub
        return null;
    }

    private class MySQLFrontendConnectionStatus extends MySQLConnectionStatus {

        @Override
        public void setNextStatus(byte packetType) {
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
                    // TODO 退出
                }
                break;
            case CMD_RECIEVED_STATUS:
                if (packetType == MySQLPacket.OK_PACKET) {
                    this.setStatus(RESULT_INIT_STATUS);
                } else if (packetType == MySQLPacket.ERROR_PACKET) {
                    this.setStatus(IDLE_STATUS);
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

}
