package org.yahaha.mq.broker.persist.constant;

import org.yahaha.mq.common.dto.req.component.MQMessage;

public class CommitLogUnit {
    private int totalSize;
    private int magicCode;
    private int queueId;
    private int bodyLength;
    private MQMessage msg;

    public CommitLogUnit(int totalSize, int magicCode, int queueId,
                         int bodyLength,MQMessage msg) {
        this.totalSize = totalSize;
        this.magicCode = magicCode;
        this.queueId = queueId;
        this.bodyLength = bodyLength;
        this.msg = msg;
    }
    
    public CommitLogUnit() {
    }

    public int getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(int totalSize) {
        this.totalSize = totalSize;
    }

    public int getMagicCode() {
        return magicCode;
    }

    public void setMagicCode(int magicCode) {
        this.magicCode = magicCode;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }

    public MQMessage getMsg() {
        return msg;
    }

    public void setMsg(MQMessage msg) {
        this.msg = msg;
    }
}
