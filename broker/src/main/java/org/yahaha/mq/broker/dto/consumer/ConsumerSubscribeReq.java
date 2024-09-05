package org.yahaha.mq.broker.dto.consumer;

import lombok.Data;
import org.yahaha.mq.common.dto.req.MQCommonReq;

@Data
public class ConsumerSubscribeReq extends MQCommonReq {

    /**
     * 分组名称
     */
    private String groupName;

    /**
     * 标题名称
     */
    private String topicName;

    /**
     * 标签正则
     */
    private String tagRegex;

    /**
     * 消费者类型
     */
    private String consumerType;
}
