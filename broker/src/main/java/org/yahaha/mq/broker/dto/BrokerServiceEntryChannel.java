package org.yahaha.mq.broker.dto;

import io.netty.channel.Channel;

public class BrokerServiceEntryChannel extends ServiceEntry {

    private Channel channel;

    /**
     * 最后访问时间
     */
    private long lastAccessTime;

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public Channel getChannel() {
        return channel;
    }

    public void setChannel(Channel channel) {
        this.channel = channel;
    }
}