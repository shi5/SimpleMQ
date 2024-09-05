package org.yahaha.mq.producer.api;

import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.producer.dto.SendBatchResult;
import org.yahaha.mq.producer.dto.SendResult;

import java.util.List;

public interface IMQProducer {
    /**
     * 同步发送消息
     * @param mqMessage 消息类型
     * @return 结果
     */
    SendResult send(final MQMessage mqMessage);

    /**
     * 单向发送消息
     * @param mqMessage 消息类型
     * @return 结果
     */
    SendResult sendOneWay(final MQMessage mqMessage);

    /**
     * 同步发送消息-批量
     * @param mqMessageList 消息类型
     * @return 结果
     */
    SendBatchResult sendBatch(final List<MQMessage> mqMessageList);

    /**
     * 单向发送消息-批量
     * @param mqMessageList 消息类型
     * @return 结果
     */
    SendBatchResult sendOneWayBatch(final List<MQMessage> mqMessageList);
}
