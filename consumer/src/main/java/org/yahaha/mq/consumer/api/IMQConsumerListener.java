package org.yahaha.mq.consumer.api;

import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.resp.ConsumerStatus;

public interface IMQConsumerListener {

    /**
     * 消费
     * @param mqMessage 消息体
     * @param context 上下文
     * @return 结果
     */
    ConsumerStatus consumer(final MQMessage mqMessage,
                            final IMQConsumerListenerContext context);
}
