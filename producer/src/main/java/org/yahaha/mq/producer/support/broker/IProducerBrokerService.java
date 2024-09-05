package org.yahaha.mq.producer.support.broker;

import io.netty.channel.Channel;
import org.yahaha.mq.common.api.Destroyable;
import org.yahaha.mq.common.dto.req.MQCommonReq;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.producer.dto.SendBatchResult;
import org.yahaha.mq.producer.dto.SendResult;

import java.util.List;

public interface IProducerBrokerService extends Destroyable {

    /**
     * 初始化列表
     * @param config 配置
     */
    void initChannelFutureList(final ProducerBrokerConfig config);

    /**
     * 注册到服务端
     */
    void registerToBroker();

    /**
     * 调用服务端
     * @param channel 调用通道
     * @param commonReq 通用请求
     * @param respClass 类
     * @param <T> 泛型
     * @param <R> 结果
     * @return 结果
     */
    <T extends MQCommonReq, R extends MQCommonResp> R callServer(Channel channel,
                                                                 T commonReq,
                                                                 Class<R> respClass);

    /**
     * 获取请求通道
     * @param key 标识
     * @return 结果
     */
    Channel getChannel(String key);

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
     * 1. 必须具有相同的 shardingKey，如果不同则忽略。
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
