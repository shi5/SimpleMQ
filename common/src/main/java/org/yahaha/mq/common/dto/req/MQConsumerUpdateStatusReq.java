package org.yahaha.mq.common.dto.req;

import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;

public class MQConsumerUpdateStatusReq extends MQCommonReq{

    private MQConsumerUpdateStatusDto consumerUpdateStatusDto;

    public MQConsumerUpdateStatusReq(MQConsumerUpdateStatusDto consumerUpdateStatusDto) {
        this.consumerUpdateStatusDto = consumerUpdateStatusDto;
    }

    public MQConsumerUpdateStatusDto getConsumerUpdateStatusDto() {
        return consumerUpdateStatusDto;
    }
    
    public void setConsumerUpdateStatusDto(MQConsumerUpdateStatusDto consumerUpdateStatusDto) {
        this.consumerUpdateStatusDto = consumerUpdateStatusDto;
    }

    @Override
    public String toString() {
        return "MqConsumerUpdateStatusReq{" +
                "consumerUpdateStatusDto='" + consumerUpdateStatusDto + '\'' +
                "} " + super.toString();
    }
}
