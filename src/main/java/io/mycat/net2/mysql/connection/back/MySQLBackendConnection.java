package io.mycat.net2.mysql.connection.back;

import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.net2.BackendConnection;
import io.mycat.net2.ByteBufferArray;
import io.mycat.net2.mysql.connection.MySQLConnection;
import io.mycat.net2.mysql.connection.MySQLConnectionStatus;
import io.mycat.net2.mysql.definination.Capabilities;
import io.mycat.net2.mysql.handler.ResponseHandler;
import io.mycat.net2.mysql.packet.AuthPacket;
import io.mycat.net2.mysql.packet.HandshakePacket;
import io.mycat.net2.mysql.packet.MySQLPacket;
import io.mycat.net2.mysql.packet.util.SecurityUtil;

public class MySQLBackendConnection extends MySQLConnection implements BackendConnection {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLBackendConnection.class);

    public static final int LOGIN_STATUS = 2; // 区分于front
    public static final int CMD_SENT_STATUS = 11;
    public static final int RESULT_WAIT_STATUS = 21;
    public static final int RESULT_INIT_STATUS = 22;
    public static final int RESULT_HEADER_STATUS = 24;
    public static final int RESULT_FETCH_STATUS = 23;
    public static final int RESULT_FAIL_STATUS = 29;
    public static final int IDLE_STATUS = -1;

    private final boolean readNode;

    private String user;
    private String password;
    private String schema;
    private ResponseHandler responseHandler;
    private PhysicalDatasource pool;

    private HandshakePacket handshake;

    private boolean authenticated;

    private long clientFlags;

    private long maxPacketSize = 16 * 1024 * 1024;

    private int charsetIndex;

    private PhysicalDatasource datasource;

    public MySQLBackendConnection(SocketChannel channel, boolean readNode) {
        super(channel);
        connectedStatus = new MySQLBackendConnectionStatus();
        this.readNode = readNode;
        this.clientFlags = initClientFlags();
    }

    private static long initClientFlags() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        boolean usingCompress = false;
        if (usingCompress) {
            flag |= Capabilities.CLIENT_COMPRESS;
        }
        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= Capabilities.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        // client extension
        flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        flag |= Capabilities.CLIENT_MULTI_RESULTS;
        return flag;
    }

    @Override
    public String getCharset() {
        return null;
    }

    private class MySQLBackendConnectionStatus extends MySQLConnectionStatus {

        @Override
        public void setNextStatus(byte packetType) {
            int status = this.getStatus();
            switch (status) {
            case LOGIN_STATUS:
                if (packetType == MySQLPacket.AUTH_PACKET) {
                    this.setStatus(IDLE_STATUS);
                    LOGGER.debug("DB status: IDLE");
                }
                break;
            case IDLE_STATUS:
                if (packetType == MySQLPacket.COM_QUERY) {
                    this.setStatus(RESULT_INIT_STATUS);
                    LOGGER.debug("DB status: Result Init");
                } else if (packetType == MySQLPacket.COM_QUIT) {
                    // TODO 退出
                }
                break;
            case RESULT_INIT_STATUS:
                if (packetType == MySQLPacket.OK_PACKET) {
                    this.setStatus(IDLE_STATUS);
                    LOGGER.debug("DB status: IDLE");
                } else if (packetType == MySQLPacket.ERROR_PACKET) {
                    this.setStatus(IDLE_STATUS);
                    LOGGER.debug("DB status: IDLE");
                } else {
                    this.setStatus(RESULT_HEADER_STATUS);
                    LOGGER.debug("DB status: Result Header");
                    LOGGER.debug("DB status: Result Field");
                }
                break;
            case RESULT_HEADER_STATUS:
                if (packetType == MySQLPacket.EOF_PACKET) {
                    this.setStatus(RESULT_FETCH_STATUS);
                    LOGGER.debug("DB status: Fetch Row");
                } else if (packetType == MySQLPacket.ERROR_PACKET) {
                    this.setStatus(IDLE_STATUS);
                    LOGGER.debug("DB status: IDLE");
                } else {
                    LOGGER.debug("DB status: Result Field");
                }
                break;
            case RESULT_FETCH_STATUS:
                if (packetType == MySQLPacket.EOF_PACKET) {
                    this.setStatus(IDLE_STATUS);
                    LOGGER.debug("DB status: IDLE");
                } else if (packetType == MySQLPacket.ERROR_PACKET) {
                    this.setStatus(IDLE_STATUS);
                    LOGGER.debug("DB status: IDLE");
                } else {
                    LOGGER.debug("DB status: Reading row");
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

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    public void setResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
    }

    public PhysicalDatasource getPool() {
        return pool;
    }

    public void setPool(PhysicalDatasource pool) {
        this.pool = pool;
    }

    public boolean isReadNode() {
        return readNode;
    }

    public HandshakePacket getHandshake() {
        return handshake;
    }

    public void setHandshake(HandshakePacket handshake) {
        this.handshake = handshake;
    }

    public boolean isAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(boolean authenticated) {
        this.authenticated = authenticated;
    }

    public void authenticate() {
        AuthPacket packet = new AuthPacket();
        packet.packetId = 1;
        packet.clientFlags = clientFlags;
        packet.maxPacketSize = maxPacketSize;
        packet.charsetIndex = this.charsetIndex;
        packet.user = user;
        try {
            packet.password = passwd(password, handshake);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
        packet.database = schema;
        ByteBufferArray bufferArray = this.getMyBufferPool().allocate();
        bufferArray.addNewBuffer();
        packet.write(bufferArray);
        // write to connection
        this.write(bufferArray);

    }

    private static byte[] passwd(String pass, HandshakePacket hs) throws NoSuchAlgorithmException {
        if (pass == null || pass.length() == 0) {
            return null;
        }
        byte[] passwd = pass.getBytes();
        int sl1 = hs.seed.length;
        int sl2 = hs.restOfScrambleBuff.length;
        byte[] seed = new byte[sl1 + sl2];
        System.arraycopy(hs.seed, 0, seed, 0, sl1);
        System.arraycopy(hs.restOfScrambleBuff, 0, seed, sl1, sl2);
        return SecurityUtil.scramble411(passwd, seed);
    }

    @Override
    public boolean isBorrowed() {
        return false;
    }

    @Override
    public boolean isClosedOrQuit() {
        return false;
    }

    @Override
    public void setBorrowed(boolean b) {
    }

    @Override
    public boolean isFromSlaveDB() {
        return false;
    }

    public PhysicalDatasource getDatasource() {
        return datasource;
    }

    public void setDatasource(PhysicalDatasource datasource) {
        this.datasource = datasource;
    }

    public void release() {
        this.datasource.releaseChannel(this);
    }

}
