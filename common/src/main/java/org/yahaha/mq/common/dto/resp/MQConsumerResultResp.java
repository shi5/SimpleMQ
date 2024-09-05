package org.yahaha.mq.common.dto.resp;

public class MQConsumerResultResp extends MQCommonResp {

    /**
     * 消费状态
     */
    private String consumerStatus;

    public String getConsumerStatus() {
        return consumerStatus;
    }

    public void setConsumerStatus(String consumerStatus) {
        this.consumerStatus = consumerStatus;
    }
}
