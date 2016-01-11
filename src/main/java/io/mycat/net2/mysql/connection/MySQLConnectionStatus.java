package io.mycat.net2.mysql.connection;

public abstract class MySQLConnectionStatus {
    private int status;

    public MySQLConnectionStatus() {
        this.reset();
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public void reset() {
        this.status = initialValue();
    }

    public abstract void setNextStatus(byte packetType);

    public abstract int initialValue();
}
