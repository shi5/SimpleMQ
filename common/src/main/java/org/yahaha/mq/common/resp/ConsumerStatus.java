package org.yahaha.mq.common.resp;

import org.yahaha.mq.common.constant.MessageStatusConst;

public enum ConsumerStatus {
    SUCCESS(MessageStatusConst.CONSUMER_SUCCESS, "消费成功"),
    FAILED(MessageStatusConst.CONSUMER_FAILED, "消费失败"),
    CONSUMER_LATER(MessageStatusConst.CONSUMER_LATER, "稍后消费"),
    ;

    private final String code;
    private final String desc;

    ConsumerStatus(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public String getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
