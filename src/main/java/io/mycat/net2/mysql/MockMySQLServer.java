package io.mycat.net2.mysql;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.net2.ExecutorUtil;
import io.mycat.net2.NIOAcceptor;
import io.mycat.net2.NIOConnector;
import io.mycat.net2.NIOReactorPool;
import io.mycat.net2.NameableExecutor;
import io.mycat.net2.NamebleScheduledExecutor;
import io.mycat.net2.NetSystem;
import io.mycat.net2.SharedBufferPool;
import io.mycat.net2.SystemConfig;
import io.mycat.net2.mysql.config.DBHostConfig;
import io.mycat.net2.mysql.connection.back.MySQLDataSource;
import io.mycat.net2.mysql.connection.back.PhysicalDBNode;
import io.mycat.net2.mysql.connection.back.PhysicalDBPool;
import io.mycat.net2.mysql.connection.back.PhysicalDatasource;
import io.mycat.net2.mysql.connection.front.MySQLFrontendConnectionFactory;

public class MockMySQLServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(MockMySQLServer.class);
    public static final int PORT = 8066;

    public static final String MOCK_HOSTNAME = "host1";

    public static final String MOCK_SCHEMA = "mycat_db1";

    public static final Map<String, PhysicalDBNode> mockDBNodes;

    static {
        mockDBNodes = new HashMap<>();
        DBHostConfig config = new DBHostConfig("host1", "127.0.0.1", 3306, "", "root", "root");
        config.setMaxCon(10);
        PhysicalDatasource dbSource = new MySQLDataSource(config, false);
        PhysicalDBPool dbPool = new PhysicalDBPool("host1", new PhysicalDatasource[] { dbSource }, new HashMap<>());
        PhysicalDBNode dbNode = new PhysicalDBNode("host1", "mycat_db1", dbPool);
        mockDBNodes.put("host1", dbNode);
    }

    public static void main(String[] args) throws IOException {
        // Business Executor ，用来执行那些耗时的任务
        NameableExecutor businessExecutor = ExecutorUtil.create("BusinessExecutor", 10);
        // 定时器Executor，用来执行定时任务
        NamebleScheduledExecutor timerExecutor = ExecutorUtil.createSheduledExecute("Timer", 5);

        SharedBufferPool sharedPool = new SharedBufferPool(1024 * 1024 * 100, 1024);
        new NetSystem(sharedPool, businessExecutor, timerExecutor);
        // Reactor pool
        NIOReactorPool reactorPool = new NIOReactorPool("Reactor Pool", 5, sharedPool);
        NIOConnector connector = new NIOConnector("NIOConnector", reactorPool);
        connector.start();
        NetSystem.getInstance().setConnector(connector);
        NetSystem.getInstance().setNetConfig(new SystemConfig());

        MySQLFrontendConnectionFactory frontFactory = new MySQLFrontendConnectionFactory();
        NIOAcceptor server = new NIOAcceptor("Server", "127.0.0.1", PORT, frontFactory, reactorPool);
        server.start();
        // server started
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());
    }
}
