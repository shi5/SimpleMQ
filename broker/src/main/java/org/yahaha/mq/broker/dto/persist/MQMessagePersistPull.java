package org.yahaha.mq.broker.dto.persist;

import lombok.Data;
import org.yahaha.mq.common.dto.req.MQPullConsumerReq;
import org.yahaha.mq.common.rpc.RpcAddress;

@Data
public class MQMessagePersistPull {
    /**
     * 消息体
     */
    private MQPullConsumerReq pullReq;

    /**
     * 地址信息
     */
    private RpcAddress rpcAddress;
}
