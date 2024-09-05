package org.yahaha.mq.common.dto.req;

import lombok.Data;

@Data
public class MQPullConsumerReq extends MQCommonReq {
    /**
     * 分组名称
     */
    private String groupName;

    /**
     * 拉取大小
     */
    private int size;

    /**
     * 标题名称
     */
    private String topic;

    /**
     * 标签正则
     */
    private String tagRegex;
    
    public MQPullConsumerReq() {
    }

    public MQPullConsumerReq(String groupName, int size, String topic, String tagRegex) {
        this.groupName = groupName;
        this.size = size;
        this.topic = topic;
        this.tagRegex = tagRegex;
    }
}
