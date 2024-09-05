package org.yahaha.mq.common.dto.req;

import org.yahaha.mq.common.dto.req.component.MQMessage;

import java.util.List;

public class MQMessageBatchReq extends MQCommonReq {
    /**
     * 消息列表
     */
    private List<MQMessage> mqMessageList;

    public List<MQMessage> getMqMessageList() {
        return mqMessageList;
    }

    public void setMqMessageList(List<MQMessage> mqMessageList) {
        this.mqMessageList = mqMessageList;
    }
}
