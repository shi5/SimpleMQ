package org.yahaha.mq.common.rpc;

import org.yahaha.mq.common.api.IServer;
import lombok.Data;

@Data
public class RpcAddress implements IServer {

    /**
     * ip地址
     */
    private String address;

    /**
     * 端口号
     */
    private int port;

    /**
     * 权重
     */
    private int weight;

    @Override
    public String url() {
        return this.address+":"+port;
    }

    @Override
    public int weight() {
        return this.weight;
    }
}
