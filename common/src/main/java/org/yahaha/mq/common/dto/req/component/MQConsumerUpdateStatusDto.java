package org.yahaha.mq.common.dto.req.component;

public class MQConsumerUpdateStatusDto {
    /**
     * 消息唯一标识
     */
    private String messageId;

    /**
     * 消息状态
     */
    private String messageStatus;

    /**
     * 消费者分组名称
     */
    private String consumerGroupName;
    
    /**
     * 主题
     */
    private String topic;
    
    /**
     * 队列ID
     */
    private int queueId;
    
    /**
     * 队列偏移量
     */
    private long offset;

    public MQConsumerUpdateStatusDto(String messageId, String messageStatus, String consumerGroupName,
                                     String topic, int queueId, long offset) {
        this.messageId = messageId;
        this.messageStatus = messageStatus;
        this.consumerGroupName = consumerGroupName;
        this.topic = topic;
        this.queueId = queueId;
        this.offset = offset;
    }

    public MQConsumerUpdateStatusDto() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getMessageStatus() {
        return messageStatus;
    }

    public void setMessageStatus(String messageStatus) {
        this.messageStatus = messageStatus;
    }

    public String getConsumerGroupName() {
        return consumerGroupName;
    }

    public void setConsumerGroupName(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "MqConsumerUpdateStatusDto{" +
                "messageId='" + messageId + '\'' +
                ", messageStatus='" + messageStatus + '\'' +
                ", consumerGroupName='" + consumerGroupName + '\'' +
                ", topic='" + topic + '\'' +
                ", queueId=" + queueId + '\'' +
                ", offset=" + offset +
                '}';
    }
}
