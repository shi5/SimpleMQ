package org.yahaha.mq.broker.api;

import io.netty.channel.Channel;
import org.yahaha.mq.broker.dto.ChannelGroupNameDto;
import org.yahaha.mq.broker.dto.ServiceEntry;
import org.yahaha.mq.broker.dto.consumer.ConsumerSubscribeBo;
import org.yahaha.mq.broker.dto.consumer.ConsumerSubscribeReq;
import org.yahaha.mq.broker.dto.consumer.ConsumerUnSubscribeReq;
import org.yahaha.mq.common.dto.req.MQHeartBeatReq;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.loadbalance.api.ILoadBalance;

import java.util.List;

public interface IBrokerConsumerService {

    /**
     * 设置负载均衡策略
     * @param loadBalance 负载均衡
     */
    void loadBalance(ILoadBalance<ConsumerSubscribeBo> loadBalance);

    /**
     * 注册当前服务信息
     * （1）将该服务通过 {@link ServiceEntry#getGroupName()} 进行分组
     * 订阅了这个 serviceId 的所有客户端
     * @param serviceEntry 注册当前服务信息
     * @param channel channel
     */
    MQCommonResp register(final ServiceEntry serviceEntry, Channel channel);

    /**
     * 注销当前服务信息
     * @param serviceEntry 注册当前服务信息
     * @param channel channel
     */
    MQCommonResp unRegister(final ServiceEntry serviceEntry, Channel channel);

    /**
     * 监听服务信息
     * （1）监听之后，如果有任何相关的机器信息发生变化，则进行推送。
     * （2）内置的信息，需要传送 ip 信息到注册中心。
     *
     * @param serviceEntry 客户端明细信息
     * @param clientChannel 客户端 channel 信息
     */
    MQCommonResp subscribe(final ConsumerSubscribeReq serviceEntry,
                           final Channel clientChannel);

    /**
     * 取消监听服务信息
     * （1）监听之后，如果有任何相关的机器信息发生变化，则进行推送。
     * （2）内置的信息，需要传送 ip 信息到注册中心。
     *
     * @param serviceEntry 客户端明细信息
     * @param clientChannel 客户端 channel 信息
     */
    MQCommonResp unSubscribe(final ConsumerUnSubscribeReq serviceEntry,
                             final Channel clientChannel);

    /**
     * 获取所有匹配的消费者-主动推送
     * 1. 同一个 groupName 只返回一个，注意负载均衡
     * 2. 返回匹配当前消息的消费者通道
     *
     * @param mqMessage 消息体
     * @return 结果
     */
    List<ChannelGroupNameDto> getPushSubscribeList(MQMessage mqMessage);

    /**
     * 心跳
     * @param mqHeartBeatReq 入参
     * @param channel 渠道
     */
    void heartbeat(final MQHeartBeatReq mqHeartBeatReq, Channel channel);
}
