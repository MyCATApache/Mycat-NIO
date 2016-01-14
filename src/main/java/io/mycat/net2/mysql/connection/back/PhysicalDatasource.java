/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.net2.mysql.connection.back;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.net2.BackendConnection;
import io.mycat.net2.NetSystem;
import io.mycat.net2.mysql.config.DBHostConfig;

public abstract class PhysicalDatasource {
    public static final Logger LOGGER = LoggerFactory.getLogger(PhysicalDatasource.class);

    private final String name;
    private final int size;
    private final DBHostConfig config;
    private final ConMap conMap = new ConMap();
    private final boolean readNode;
    private volatile long heartbeatRecoveryTime;
    private PhysicalDBPool dbPool;

    public PhysicalDatasource(DBHostConfig config, // DataHostConfig hostConfig,
            boolean isReadNode) {
        this.size = config.getMaxCon();
        this.config = config;
        this.name = config.getHostName();
        this.readNode = isReadNode;
    }

    public boolean isMyConnection(BackendConnection con) {
        return (con.getPool() == this);
    }

    public boolean isReadNode() {
        return readNode;
    }

    public int getSize() {
        return size;
    }

    public void setDbPool(PhysicalDBPool dbPool) {
        this.dbPool = dbPool;
    }

    public PhysicalDBPool getDbPool() {
        return dbPool;
    }

    // public abstract DBHeartbeat createHeartBeat();

    public String getName() {
        return name;
    }

    public int getIndex() {
        int currentIndex = 0;
        for (int i = 0; i < dbPool.getSources().length; i++) {
            PhysicalDatasource writeHostDatasource = dbPool.getSources()[i];
            if (writeHostDatasource.getName().equals(getName())) {
                currentIndex = i;
                break;
            }
        }
        return currentIndex;
    }

    public boolean isSalveOrRead() {
        int currentIndex = getIndex();
        if (currentIndex != dbPool.activedIndex || this.readNode) {
            return true;
        }
        return false;
    }

    public long getExecuteCount() {
        long executeCount = 0;
        for (ConQueue queue : conMap.getAllConQueue()) {
            executeCount += queue.getExecuteCount();
        }
        return executeCount;
    }

    public long getExecuteCountForSchema(String schema) {
        return conMap.getSchemaConQueue(schema).getExecuteCount();

    }

    public int getActiveCountForSchema(String schema) {
        return conMap.getActiveCountForSchema(schema, this);
    }

    public int getIdleCountForSchema(String schema) {
        ConQueue queue = conMap.getSchemaConQueue(schema);
        int total = 0;
        total += queue.getAutoCommitCons().size() + queue.getManCommitCons().size();
        return total;
    }

    // private boolean validSchema(String schema) {
    // String theSchema = schema;
    // return theSchema != null & !"".equals(theSchema) &&
    // !"snyn...".equals(theSchema);
    // }
    //
    // private void closeByIdleMany(int ildeCloseCount) {
    // LOGGER.info("too many ilde cons ,close some for datasouce " + name);
    // List<BackendConnection> readyCloseCons = new
    // ArrayList<BackendConnection>(ildeCloseCount);
    // for (ConQueue queue : conMap.getAllConQueue()) {
    // readyCloseCons.addAll(queue.getIdleConsToClose(ildeCloseCount));
    // if (readyCloseCons.size() >= ildeCloseCount) {
    // break;
    // }
    // }
    //
    // for (BackendConnection idleCon : readyCloseCons) {
    // if (idleCon.isBorrowed()) {
    // LOGGER.warn("find idle con is using " + idleCon);
    // }
    // idleCon.close("too many idle con");
    // }
    // }

    // private void createByIdleLitte(int idleCons, int createCount) {
    // LOGGER.info("create connections ,because idle connection not enough ,cur
    // is " + idleCons + ", for " + name);
    // final String[] schemas = dbPool.getSchemas();
    // for (int i = 0; i < createCount; i++) {
    // if (this.getActiveCount() + this.getIdleCount() >= size) {
    // break;
    // }
    // try {
    // // creat new connection
    // this.createNewConnection(null, schemas[i % schemas.length]);
    // } catch (IOException e) {
    // LOGGER.warn("create connection err " + e);
    // }
    //
    // }
    // }

    public int getActiveCount() {
        return this.conMap.getActiveCountForDs(this);
    }

    public void clearCons(String reason) {
        this.conMap.clearConnections(reason, this);
    }

    private BackendConnection takeCon(BackendConnection conn, final Object attachment, String schema) {

        conn.setBorrowed(true);
        if (!conn.getSchema().equals(schema)) {
            // need do schema syn in before sql send
            conn.setSchema(schema);
        }
        ConQueue queue = conMap.getSchemaConQueue(schema);
        queue.incExecuteCount();
        return conn;
    }

    private BackendConnection createNewConnection(final Object attachment, final String schema) throws IOException {
        // aysn create connection
        BackendConnection con = createNewConnection(schema);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        con.setDatasource(this);
        this.conMap.getSchemaConQueue(schema).getAutoCommitCons().add(con);
        return con;
    }

    public BackendConnection getConnection(String schema, boolean autocommit, final Object attachment)
            throws IOException {
        BackendConnection con = this.conMap.tryTakeCon(schema, autocommit);
        if (con != null) {
            takeCon(con, // handler,
                    attachment, schema);
            return con;
        } else {
            int activeCons = this.getActiveCount();// 当前最大活动连接
            if (activeCons + 1 > size) {// 下一个连接大于最大连接数
                LOGGER.error("the max activeConnnections size can not be max than maxconnections");
                throw new IOException("the max activeConnnections size can not be max than maxconnections");
            } else { // create connection
                LOGGER.info(
                        "no ilde connection in pool,create new connection for " + this.name + " of schema " + schema);
                return createNewConnection(// handler,
                        attachment, schema);
            }
        }

    }

    private void returnCon(BackendConnection c) {
        // c.setAttachment(null);
        c.setBorrowed(false);
        // c.setLastTime(TimeUtil.currentTimeMillis());
        ConQueue queue = this.conMap.getSchemaConQueue(c.getSchema());

        boolean ok = false;
        // if (c.isAutocommit()) {
        // ok = queue.getAutoCommitCons().offer(c);
        // } else {
        ok = queue.getManCommitCons().offer(c);
        // }
        if (!ok) {

            LOGGER.warn("can't return to pool ,so close con " + c);
            // c.close("can't return to pool ");
        }
    }

    public void releaseChannel(BackendConnection c) {
        returnCon(c);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("release channel " + c);
        }
    }

    public void connectionClosed(BackendConnection conn) {
        ConQueue queue = this.conMap.getSchemaConQueue(conn.getSchema());
        if (queue != null) {
            queue.removeCon(conn);
        }

    }

    public abstract BackendConnection createNewConnection(String schema) throws IOException;

    public long getHeartbeatRecoveryTime() {
        return heartbeatRecoveryTime;
    }

    public void setHeartbeatRecoveryTime(long heartbeatRecoveryTime) {
        this.heartbeatRecoveryTime = heartbeatRecoveryTime;
    }

    public DBHostConfig getConfig() {
        return config;
    }
}