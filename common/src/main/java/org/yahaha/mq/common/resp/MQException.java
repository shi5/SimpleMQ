package org.yahaha.mq.common.resp;

public class MQException extends RuntimeException implements RespCode{
    private final RespCode respCode;

    public MQException(RespCode respCode) {
        this.respCode = respCode;
    }

    public MQException(String message, RespCode respCode) {
        super(message);
        this.respCode = respCode;
    }

    public MQException(String message, Throwable cause, RespCode respCode) {
        super(message, cause);
        this.respCode = respCode;
    }

    public MQException(Throwable cause, RespCode respCode) {
        super(cause);
        this.respCode = respCode;
    }

    public MQException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace, RespCode respCode) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.respCode = respCode;
    }

    @Override
    public String getCode() {
        return this.respCode.getCode();
    }

    @Override
    public String getMsg() {
        return this.respCode.getMsg();
    }
}
