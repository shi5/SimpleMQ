package org.yahaha.mq.broker.constant;

import org.yahaha.mq.common.resp.RespCode;

public enum BrokerRespCode implements RespCode {

    RPC_INIT_FAILED("B00001", "中间人启动失败"),
    MSG_PUSH_FAILED("B00002", "中间人消息推送失败"),
    ;

    private final String code;
    private final String msg;

    BrokerRespCode(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getMsg() {
        return msg;
    }
}
