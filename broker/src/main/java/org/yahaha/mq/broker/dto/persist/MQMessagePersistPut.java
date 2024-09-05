package org.yahaha.mq.broker.dto.persist;

import lombok.Data;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.rpc.RpcAddress;

@Data
public class MQMessagePersistPut {
    /**
     * 消息体
     */
    private MQMessage mqMessage;

    /**
     * 地址信息
     */
    private RpcAddress rpcAddress;

    /**
     * 主题
     */
    private String topic;

    /**
     * 队列ID
     */
    private int queueId;

    public MQMessagePersistPut() {
    }

}
