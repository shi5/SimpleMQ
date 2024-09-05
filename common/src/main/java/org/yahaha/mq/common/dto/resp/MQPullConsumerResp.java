package org.yahaha.mq.common.dto.resp;

import org.yahaha.mq.common.dto.req.component.MQMessageExt;

import java.util.List;

public class MQPullConsumerResp extends MQCommonResp {

    /**
     * 消息列表
     */
    private List<MQMessageExt> list;
    
    public MQPullConsumerResp() {
    }
    
    public MQPullConsumerResp(List<MQMessageExt> list) {
        this.list = list;
    }

    public List<MQMessageExt> getList() {
        return list;
    }

    public void setList(List<MQMessageExt> list) {
        this.list = list;
    }

    @Override
    public String toString() {
        return "MqConsumerPullResp{" +
                "list=" + list +
                "} " + super.toString();
    }

}
