package io.mycat.net2.mysql.connection.back;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import io.mycat.net2.NetSystem;
import io.mycat.net2.mysql.config.DBHostConfig;

public class MySQLBackendConnectionFactory {
    private final MySQLBackendConnectionHandler nioHandler = new MySQLBackendConnectionHandler();

    public MySQLBackendConnection make(MySQLDataSource pool, String schema) throws IOException {

        DBHostConfig dsc = pool.getConfig();
        SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);

        MySQLBackendConnection c = new MySQLBackendConnection(channel, pool.isReadNode());
        NetSystem.getInstance().setSocketParams(c, false);
        // 设置NIOHandler
        c.setHandler(nioHandler);
        c.setHost(dsc.getIp());
        c.setPort(dsc.getPort());
        c.setUser(dsc.getUser());
        c.setPassword(dsc.getPassword());
        c.setSchema(schema);
        c.setPool(pool);
        c.setIdleTimeout(pool.getConfig().getIdleTimeout());
        NetSystem.getInstance().getConnector().postConnect(c);
        return c;
    }
}
