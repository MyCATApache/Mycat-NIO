package io.mycat.net2.mysql.connection.front;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import io.mycat.net2.Connection;
import io.mycat.net2.ConnectionFactory;
import io.mycat.net2.NIOHandler;

public class MySQLFrontendConnectionFactory extends ConnectionFactory {

    private NIOHandler handler = new MySQLFrontConnectionHandler();

    @Override
    protected Connection makeConnection(SocketChannel channel) throws IOException {
        MySQLFrontendConnection con = new MySQLFrontendConnection(channel);
        // con.setPrivileges(MycatPrivileges.instance());
        // con.setCharset("UTF-8");
        // con.setLoadDataInfileHandler(new ServerLoadDataInfileHandler(c));
        // c.setPrepareHandler(new ServerPrepareHandler(c));
        // con.setTxIsolation(sys.getTxIsolation());
        return con;
    }

    @Override
    protected NIOHandler getNIOHandler() {
        return handler;
    }

}
