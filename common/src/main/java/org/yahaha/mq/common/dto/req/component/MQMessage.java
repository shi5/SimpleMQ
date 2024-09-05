package org.yahaha.mq.common.dto.req.component;

import lombok.Data;
import org.yahaha.mq.common.dto.req.MQCommonReq;

@Data
public class MQMessage extends MQCommonReq {
    /**
     * 分组名称
     */
    private String groupName;

    /**
     * 标题名称
     */
    private String topic;

    /**
     * 标签
     */
    private String tag;

    /**
     * 内容
     */
    private String payload;
//
//    /**
//     * 业务标识
//     */
//    private String bizKey;

    /**
     * 负载分片标识
     */
    private String shardingKey;

    // getter&setter&toString
}
