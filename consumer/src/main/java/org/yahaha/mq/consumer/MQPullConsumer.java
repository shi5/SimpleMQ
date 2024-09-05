package org.yahaha.mq.consumer;

import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.common.constant.ConsumerTypeConst;
import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.req.component.MQMessageExt;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.dto.resp.MQPullConsumerResp;
import org.yahaha.mq.common.resp.ConsumerStatus;
import org.yahaha.mq.common.resp.MQCommonRespCode;
import org.yahaha.mq.consumer.api.IMQConsumerListenerContext;
import org.yahaha.mq.consumer.dto.MQTopicTagDto;
import org.yahaha.mq.consumer.support.listener.MQConsumerListenerContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MQPullConsumer extends MQPushConsumer{
    /**
     * 拉取定时任务
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    /**
     * 单次拉取大小
     */
    private int size = 10;

    /**
     * 初始化延迟数
     */
    private int pullInitDelaySeconds = 5;

    /**
     * 拉取周期
     */
    private int pullPeriodSeconds = 5;

    /**
     * 状态回执是否批量
     */
    private boolean ackBatchFlag = true;

    /**
     * 订阅列表
     */
    private final List<MQTopicTagDto> subscribeList = new ArrayList<>();

    public MQPullConsumer size(int size) {
        this.size = size;
        return this;
    }

    public MQPullConsumer pullInitDelaySeconds(int pullInitDelaySeconds) {
        this.pullInitDelaySeconds = pullInitDelaySeconds;
        return this;
    }

    public MQPullConsumer pullPeriodSeconds(int pullPeriodSeconds) {
        this.pullPeriodSeconds = pullPeriodSeconds;
        return this;
    }

    public MQPullConsumer ackBatchFlag(boolean ackBatchFlag) {
        this.ackBatchFlag = ackBatchFlag;
        return this;
    }

    /**
     * 初始化拉取消息
     */
    @Override
    public void afterInit() {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if(CollectionUtil.isEmpty(subscribeList)) {
                    log.warn("订阅列表为空，忽略处理。");
                    return;
                }

                for(MQTopicTagDto tagDto : subscribeList) {
                    final String topicName = tagDto.getTopicName();
                    final String tagRegex = tagDto.getTagRegex();

                    MQPullConsumerResp resp = consumerBrokerService.pull(topicName, tagRegex, size);

                    if(MQCommonRespCode.SUCCESS.getCode().equals(resp.getRespCode())) {
                        List<MQMessageExt> mqMessageList = resp.getList();
                        if(CollectionUtil.isNotEmpty(mqMessageList)) {
                            List<MQConsumerUpdateStatusDto> statusDtoList = new ArrayList<>(mqMessageList.size());
                            for(MQMessageExt mqMessageExt : mqMessageList) {
                                final MQMessage mqMessage = mqMessageExt.getMqMessage();
                                IMQConsumerListenerContext context = new MQConsumerListenerContext();
                                final String messageId = mqMessage.getTraceId();
                                ConsumerStatus consumerStatus = mqListenerService.consumer(mqMessage, context);
                                log.info("消息：{} 消费结果 {}", messageId, consumerStatus);

                                MQConsumerUpdateStatusDto statusDto = new MQConsumerUpdateStatusDto(mqMessage.getTraceId(),
                                        consumerStatus.getCode(), groupName, topicName,
                                        mqMessageExt.getQueueId(), mqMessageExt.getOffset());
                                // 状态同步更新
                                if(!ackBatchFlag) {
                                    
                                    MQCommonResp ackResp = consumerBrokerService.consumerStatusAck(statusDto);
                                    log.info("消息：{} 状态回执结果 {}", messageId, JSON.toJSON(ackResp));
                                } else {
                                    // 批量
                                    statusDtoList.add(statusDto);
                                }
                            }

                            // 批量执行
                            if(ackBatchFlag) {
                                MQCommonResp ackResp = consumerBrokerService.consumerStatusAckBatch(statusDtoList);
                                log.info("消息：{} 状态批量回执结果 {}", statusDtoList, JSON.toJSON(ackResp));
                                statusDtoList = null;
                            }
                        }
                    } else {
                        log.error("拉取消息失败: {}", JSON.toJSON(resp));
                    }
                }
            }
        }, pullInitDelaySeconds, pullPeriodSeconds, TimeUnit.SECONDS);
    }

    @Override
    protected String getConsumerType() {
        return ConsumerTypeConst.PULL;
    }

    @Override
    public synchronized void subscribe(String topicName, String tagRegex) {
        MQTopicTagDto tagDto = buildMQTopicTagDto(topicName, tagRegex);

        if(!subscribeList.contains(tagDto)) {
            subscribeList.add(tagDto);
        }
    }

    @Override
    public void unSubscribe(String topicName, String tagRegex) {
        MQTopicTagDto tagDto = buildMQTopicTagDto(topicName, tagRegex);

        subscribeList.remove(tagDto);
    }

    private MQTopicTagDto buildMQTopicTagDto(String topicName, String tagRegex) {
        MQTopicTagDto dto = new MQTopicTagDto();
        dto.setTagRegex(tagRegex);
        dto.setTopicName(topicName);
        return dto;
    }
}
