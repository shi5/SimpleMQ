package org.yahaha.mq.common.dto.req;

import lombok.Data;

import java.io.Serializable;

@Data
public class MQCommonReq implements Serializable {

    /**
     * 请求标识
     */
    private String traceId;

    /**
     * 方法类型
     */
    private String methodType;

}
