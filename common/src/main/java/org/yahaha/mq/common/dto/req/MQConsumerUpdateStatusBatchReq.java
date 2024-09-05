package org.yahaha.mq.common.dto.req;

import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;

import java.util.List;

public class MQConsumerUpdateStatusBatchReq extends MQCommonReq {
    private List<MQConsumerUpdateStatusDto> statusList;

    public List<MQConsumerUpdateStatusDto> getStatusList() {
        return statusList;
    }

    public void setStatusList(List<MQConsumerUpdateStatusDto> statusList) {
        this.statusList = statusList;
    }
}
