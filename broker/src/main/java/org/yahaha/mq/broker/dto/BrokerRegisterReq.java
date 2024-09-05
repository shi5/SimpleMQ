package org.yahaha.mq.broker.dto;

import lombok.Data;
import org.yahaha.mq.common.dto.req.MQCommonReq;

@Data
public class BrokerRegisterReq extends MQCommonReq {

    /**
     * 服务信息
     */
    private ServiceEntry serviceEntry;

    /**
     * 账户标识
     */
    private String appKey;

    /**
     * 账户密码
     */
    private String appSecret;

}
