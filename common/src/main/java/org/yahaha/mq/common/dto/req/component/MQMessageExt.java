package org.yahaha.mq.common.dto.req.component;

public class MQMessageExt{
    private long offset;
    private int queueId;
    private MQMessage message;

    public MQMessageExt() {
    }

    public MQMessageExt(long offset, int queueId, MQMessage message) {
        this.offset = offset;
        this.queueId = queueId;
        this.message = message;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
    
    public MQMessage getMqMessage() {
        return message;
    }

    public void setMqMessage(MQMessage message) {
        this.message = message;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }
}
