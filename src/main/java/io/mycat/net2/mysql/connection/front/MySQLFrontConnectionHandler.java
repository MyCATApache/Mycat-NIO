package io.mycat.net2.mysql.connection.front;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.net2.BackendConnection;
import io.mycat.net2.ByteBufferArray;
import io.mycat.net2.Connection;
import io.mycat.net2.NIOHandler;
import io.mycat.net2.mysql.MockMySQLServer;
import io.mycat.net2.mysql.connection.back.MySQLBackendConnection;
import io.mycat.net2.mysql.definination.ErrorCode;
import io.mycat.net2.mysql.handler.ResponseHandler;
import io.mycat.net2.mysql.handler.SelectHandler;
import io.mycat.net2.mysql.packet.AuthPacket;
import io.mycat.net2.mysql.packet.EOFPacket;
import io.mycat.net2.mysql.packet.FieldPacket;
import io.mycat.net2.mysql.packet.MySQLMessage;
import io.mycat.net2.mysql.packet.MySQLPacket;
import io.mycat.net2.mysql.packet.ResultSetHeaderPacket;
import io.mycat.net2.mysql.packet.RowDataPacket;
import io.mycat.net2.mysql.packet.util.CommonPacketUtil;
import io.mycat.net2.mysql.parser.ServerParse;

public class MySQLFrontConnectionHandler implements NIOHandler<MySQLFrontendConnection> {
    private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLFrontConnectionHandler.class);

    @Override
    public void onConnected(MySQLFrontendConnection con) throws IOException {
        LOGGER.debug("onConnected", con);
        con.sendAuthPackge();
    }

    @Override
    public void onConnectFailed(MySQLFrontendConnection con, Throwable e) {
        LOGGER.debug("onConnectFailed", con);
    }

    @Override
    public void onClosed(MySQLFrontendConnection con, String reason) {
        LOGGER.debug("onClosed", con);
    }

    @Override
    public void handle(MySQLFrontendConnection con, ByteBufferArray bufferArray) {
        // TODO 未处理粘包
        LOGGER.debug("handle", con);
        int connectedStatus = con.getConnectedStatus();
        int packetIndex = bufferArray.getCurPacageIndex() - 1;
        switch (connectedStatus) {
        case MySQLFrontendConnection.LOGIN_STATUS:
            LOGGER.debug("Handle Login msg");
            doLogin(con, bufferArray, packetIndex);
            break;
        case MySQLFrontendConnection.CMD_RECIEVED_STATUS:
            LOGGER.debug("Handle Query msg");
            doQuery(con, bufferArray, packetIndex);
            break;
        case MySQLFrontendConnection.QUIT_STATUS:
            LOGGER.info("Client quit.");
            con.close("quit packet");
            break;
        }
    }

    private void doQuery(MySQLFrontendConnection con, ByteBufferArray bufferArray, final int packetIndex) {
        // 取得语句
        MySQLMessage mm = new MySQLMessage(bufferArray, packetIndex);
        mm.position(5);
        String sql = null;
        sql = mm.readString();
        LOGGER.debug(sql);
        // 执行查询
        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;

        // 检查当前使用的DB
        // String db = "";
        // if (db == null
        // && sqlType!=ServerParse.USE
        // && sqlType!=ServerParse.HELP
        // && sqlType!=ServerParse.SET
        // && sqlType!=ServerParse.SHOW
        // && sqlType!=ServerParse.KILL
        // && sqlType!=ServerParse.KILL_QUERY
        // && sqlType!=ServerParse.MYSQL_COMMENT
        // && sqlType!=ServerParse.MYSQL_CMD_COMMENT) {
        // writeErrMessage(ErrorCode.ERR_BAD_LOGICDB, "No MyCAT Database
        // selected");
        // return;
        // }

        switch (sqlType) {
        case ServerParse.EXPLAIN:
            LOGGER.debug("EXPLAIN");
            break;
        case ServerParse.SET:
            LOGGER.debug("SET");
            break;
        case ServerParse.SHOW:
            LOGGER.debug("SHOW");
            break;
        case ServerParse.SELECT:
            LOGGER.debug("SELECT");
            // SelectHandler.handle(sql, con, rs >>> 8);
            break;
        case ServerParse.START:
            LOGGER.debug("START");
            break;
        case ServerParse.BEGIN:
            LOGGER.debug("BEGIN");
            break;
        case ServerParse.SAVEPOINT:
            LOGGER.debug("SAVEPOINT");
            break;
        case ServerParse.KILL:
            LOGGER.debug("KILL");
            break;
        case ServerParse.KILL_QUERY:
            LOGGER.debug("KILL_QUERY");
            break;
        case ServerParse.USE:
            LOGGER.debug("USE");
            break;
        case ServerParse.COMMIT:
            LOGGER.debug("COMMIT");
            break;
        case ServerParse.ROLLBACK:
            LOGGER.debug("ROLLBACK");
            break;
        case ServerParse.HELP:
            LOGGER.debug("HELP");
            break;
        case ServerParse.MYSQL_CMD_COMMENT:
            LOGGER.debug("MYSQL_CMD_COMMENT");
            break;
        case ServerParse.MYSQL_COMMENT:
            LOGGER.debug("MYSQL_COMMENT");
            break;
        case ServerParse.LOAD_DATA_INFILE_SQL:
            LOGGER.debug("LOAD_DATA_INFILE_SQL");
            break;
        default:
            LOGGER.debug("DEFAULT");
        }
        if (sql.equals("select @@version_comment limit 1")) {
            SelectHandler.handle(sql, con, rs >>> 8);
            return;
        }
        // TODO MOCK
        try {
            BackendConnection backCon = MockMySQLServer.mockDBNodes.get(MockMySQLServer.MOCK_HOSTNAME)
                    .getConnection(MockMySQLServer.MOCK_SCHEMA, true, null);
            backCon.setResponseHandler(new ResponseHandler() {

                private ByteBufferArray newBufferArray;

                private ByteBuffer newBuffer;

                @Override
                public void connectionError(Throwable e, BackendConnection conn) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void connectionClose(BackendConnection conn, String reason) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void connectionAcquired(BackendConnection conn) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void handleResponse(ByteBufferArray bufferArray, BackendConnection conn) {
                    // Mock 直接转发给前端
                    // IMPORTANT!! 处理粘包
                    int lastPacketIndex = bufferArray.getCurPacageIndex() - 1;
                    if (newBufferArray == null) {
                        newBufferArray = con.getMyBufferPool().allocate();
                        newBuffer = newBufferArray.addNewBuffer();
                    }
                    for (int i = bufferArray.getCurHandlingPacageIndex(); i <= lastPacketIndex; i++) {
                        newBuffer = newBufferArray.checkWriteBuffer(bufferArray.getPacageLength(i));
                        sendPacket(newBuffer, bufferArray, i);
                    }
                    bufferArray.setCurHandlingPacageIndex(lastPacketIndex + 1);
                }

                @Override
                public void finishResponse(BackendConnection conn) {
                    con.write(newBufferArray);
                    conn.release();
                }
            });
            // 后面需要加入handler
            MySQLBackendConnection mysqlBackCon = (MySQLBackendConnection) backCon;
            ByteBufferArray newBufferArray = mysqlBackCon.getMyBufferPool().allocate();
            ByteBuffer newBuffer = newBufferArray.addNewBuffer();
            sendPacket(newBuffer, bufferArray, packetIndex);
            mysqlBackCon.write(newBufferArray);
        } catch (Exception e) {
            LOGGER.error("Error", e);
        }
    }

    private void sendPacket(ByteBuffer newBuffer, ByteBufferArray bufferArray, int packetIndex) {
        for (int i = 0; i < bufferArray.getPacageLength(packetIndex); i++) {
            newBuffer.put(bufferArray.readPacket(packetIndex, i));
        }
    }

    private void doLogin(MySQLFrontendConnection con, ByteBufferArray bufferArray, int packetIndex) {
        byte type = bufferArray.getCurPacageType();
        // check quit packet
        if (type == MySQLPacket.QUIT_PACKET) {
            con.close("quit packet");
            return;
        }

        AuthPacket auth = new AuthPacket();
        auth.read(bufferArray, packetIndex);

        // Fake check user
        LOGGER.debug("Check user name. " + auth.user);
        if (!auth.user.equals("root")) {
            LOGGER.debug("User name error. " + auth.user);
            failure(con, ErrorCode.ER_ACCESS_DENIED_ERROR, "Access denied for user '" + auth.user + "'");
            return;
        }

        // Fake check password
        LOGGER.debug("Check user password. " + new String(auth.password));

        // check schema
        LOGGER.debug("Check database. " + auth.database);
        success(con, auth);
    }

    protected void failure(MySQLFrontendConnection source, int errno, String info) {
        LOGGER.error(source.toString() + info);
        source.writeErrMessage(errno, info);
    }

    protected void success(MySQLFrontendConnection con, AuthPacket auth) {
        LOGGER.debug("Login success.");
        // con.setAuthenticated(true);
        // con.setUser(auth.user);
        // con.setSchema(auth.database);
        // con.setCharsetIndex(auth.charsetIndex);

        con.write(AUTH_OK);
        con.setState(Connection.State.connected);
        con.setConnectedStatus(MySQLFrontendConnection.IDLE_STATUS);
    }

}
