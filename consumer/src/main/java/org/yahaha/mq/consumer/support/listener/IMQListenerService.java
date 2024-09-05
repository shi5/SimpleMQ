package org.yahaha.mq.consumer.support.listener;

import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.resp.ConsumerStatus;
import org.yahaha.mq.consumer.api.IMQConsumerListener;
import org.yahaha.mq.consumer.api.IMQConsumerListenerContext;

public interface IMQListenerService {
    /**
     * 注册
     * @param listener 监听器
     */
    void register(final IMQConsumerListener listener);

    /**
     * 消费消息
     * @param mqMessage 消息
     * @param context 上下文
     * @return 结果
     */
    ConsumerStatus consumer(final MQMessage mqMessage,
                            final IMQConsumerListenerContext context);
}
