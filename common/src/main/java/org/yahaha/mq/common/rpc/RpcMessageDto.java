package org.yahaha.mq.common.rpc;

import lombok.Data;
import org.yahaha.mq.common.resp.MQCommonRespCode;

import java.io.Serializable;

@Data
public class RpcMessageDto implements Serializable {

    /**
     * 请求时间
     */
    private long requestTime;

    /**
     * 请求标识
     */
    private String traceId;

    /**
     * 方法类型
     */
    private String methodType;

    /**
     * 是否为请求消息
     */
    private boolean isRequest;

    private String respCode;

    private String respMsg;

    private String json;


    public static RpcMessageDto timeout() {
        RpcMessageDto dto = new RpcMessageDto();
        dto.setRespCode(MQCommonRespCode.TIMEOUT.getCode());
        dto.setRespMsg(MQCommonRespCode.TIMEOUT.getMsg());

        return dto;
    }


}
