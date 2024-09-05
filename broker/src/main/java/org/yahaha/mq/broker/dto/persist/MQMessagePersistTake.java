package org.yahaha.mq.broker.dto.persist;

import lombok.Data;

import java.util.List;

@Data
public class MQMessagePersistTake {
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
    private List<String> tags;
}
