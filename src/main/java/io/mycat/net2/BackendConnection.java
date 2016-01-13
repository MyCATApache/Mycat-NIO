package io.mycat.net2;

import io.mycat.net2.mysql.connection.back.PhysicalDatasource;
import io.mycat.net2.mysql.handler.ResponseHandler;

public interface BackendConnection extends ClosableConnection {

    public abstract String getSchema();

    public abstract void setSchema(String newSchema);

    public abstract boolean isBorrowed();

    public abstract PhysicalDatasource getPool();

    public abstract boolean isClosedOrQuit();

    public abstract void setBorrowed(boolean b);

    public abstract boolean isFromSlaveDB();

    public abstract void setResponseHandler(ResponseHandler commandHandler);
    
    public abstract void release();
    
    public abstract void setDatasource(PhysicalDatasource datasource);
}
