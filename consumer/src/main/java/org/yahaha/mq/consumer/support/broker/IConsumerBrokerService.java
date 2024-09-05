package org.yahaha.mq.consumer.support.broker;

import io.netty.channel.Channel;
import org.yahaha.mq.common.api.Destroyable;
import org.yahaha.mq.common.dto.req.MQCommonReq;
import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.dto.resp.MQPullConsumerResp;

import java.util.List;

public interface IConsumerBrokerService extends Destroyable {

    /**
     * 初始化列表
     * @param config 配置
     */
    void initChannelFutureList(final ConsumerBrokerConfig config);

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
     * 订阅
     * @param topicName topic 名称
     * @param tagRegex 标签正则
     */
    void subscribe(String topicName, String tagRegex);

    /**
     * 取消订阅
     * @param topicName topic 名称
     * @param tagRegex 标签正则
     */
    void unSubscribe(String topicName, String tagRegex);

    /**
     * 心跳
     */
    void heartbeat();

    /**
     * 拉取消息
     * @param topicName 标题名称
     * @param tagRegex 标签正则
     * @param fetchSize 大小
     * @return 结果
     */
    MQPullConsumerResp pull(String topicName,
                            String tagRegex,
                            int fetchSize);

    /**
     * 消费状态回执
     * @param statusDto 状态
     * @return 结果
     */
    MQCommonResp consumerStatusAck(MQConsumerUpdateStatusDto statusDto);

    /**
     * 消费状态回执-批量
     * @param statusDtoList 状态列表
     * @return 结果
     */
    MQCommonResp consumerStatusAckBatch(List<MQConsumerUpdateStatusDto> statusDtoList);

}
