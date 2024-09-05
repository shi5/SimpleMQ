package org.yahaha.mq.broker.persist;

import io.netty.channel.Channel;
import org.yahaha.mq.broker.dto.persist.MQMessagePersistPut;
import org.yahaha.mq.common.dto.req.MQPullConsumerReq;
import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.dto.resp.MQPullConsumerResp;

import java.util.List;

public interface IMQBrokerPersist {

    /**
     * 保存消息
     * @param mqMessage 消息
     */
    MQCommonResp putMessage(final MQMessagePersistPut mqMessage);

    /**
     * 保存消息-批量
     * @param putList 消息
     * @return 响应
     */
    // TODO: 通过列表这样批量是否可以优化
    MQCommonResp putMessageBatch(final List<MQMessagePersistPut> putList);

    /**
     * 更新状态
     * @param statusDto 更新状态
     * @return 结果
     */
    MQCommonResp updateStatus(MQConsumerUpdateStatusDto statusDto);

    /**
     * 更新状态-批量
     * @param statusDtoList 状态列表
     * @return 结果
     */
    MQCommonResp updateStatusBatch(List<MQConsumerUpdateStatusDto> statusDtoList);

    /**
     * 拉取消息
     * @param pullReq 拉取消息
     * @param channel 通道
     * @return 结果
     */
    MQPullConsumerResp pull(final MQPullConsumerReq pullReq, final Channel channel);

    /**
     * 获取队列数量
     * @param topic 主题
     * @return 队列数量
     */
    int getConsumeQueueNum(String topic);
    
}
