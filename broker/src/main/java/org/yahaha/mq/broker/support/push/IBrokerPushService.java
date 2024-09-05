package org.yahaha.mq.broker.support.push;

public interface IBrokerPushService {

    /**
     * 异步推送
     * @param context 消息
     */
    void asyncPush(final BrokerPushContext context);

}
