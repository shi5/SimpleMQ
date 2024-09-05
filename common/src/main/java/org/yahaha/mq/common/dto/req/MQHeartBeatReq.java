package org.yahaha.mq.common.dto.req;

public class MQHeartBeatReq extends MQCommonReq {

    /**
     * address ip地址
     */
    private String address;

    /**
     * 端口号
     */
    private int port;

    /**
     * 请求时间
     */
    private long time;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
