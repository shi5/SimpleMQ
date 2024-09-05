package org.yahaha.mq.broker.api;

import io.netty.channel.Channel;
import org.yahaha.mq.broker.dto.ServiceEntry;
import org.yahaha.mq.common.dto.resp.MQCommonResp;

public interface IBrokerProducerService {
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
     * @param channel 通道
     */
    MQCommonResp unRegister(final ServiceEntry serviceEntry, Channel channel);

    /**
     * 获取服务地址信息
     * @param channelId
     * @return 结果
     */
    ServiceEntry getServiceEntry(String channelId);
}
