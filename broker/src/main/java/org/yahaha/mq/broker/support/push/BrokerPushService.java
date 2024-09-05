package org.yahaha.mq.broker.support.push;

import com.alibaba.fastjson.JSON;
import com.github.houbb.sisyphus.core.core.Retryer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.constant.BrokerRespCode;
import org.yahaha.mq.broker.dto.ChannelGroupNameDto;
import org.yahaha.mq.broker.dto.persist.MQMessagePersistPut;
import org.yahaha.mq.broker.persist.IMQBrokerPersist;
import org.yahaha.mq.common.constant.MessageStatusConst;
import org.yahaha.mq.common.constant.MethodType;
import org.yahaha.mq.common.dto.req.MQCommonReq;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.dto.resp.MQConsumerResultResp;
import org.yahaha.mq.common.resp.ConsumerStatus;
import org.yahaha.mq.common.resp.MQCommonRespCode;
import org.yahaha.mq.common.resp.MQException;
import org.yahaha.mq.common.rpc.RpcMessageDto;
import org.yahaha.mq.common.support.invoke.IInvokeService;
import org.yahaha.mq.common.util.ChannelUtil;
import org.yahaha.mq.common.util.DelimiterUtil;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class BrokerPushService implements IBrokerPushService {

    private static final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();

    @Override
    public void asyncPush(final BrokerPushContext context) {
        EXECUTOR_SERVICE.submit(new Runnable() {
            @Override
            public void run() {
                log.info("开始异步处理 {}", JSON.toJSON(context));
                final MQMessagePersistPut persistPut = context.mqMessagePersistPut();
                final MQMessage mqMessage = persistPut.getMqMessage();
                final List<ChannelGroupNameDto> channelList = context.channelList();
                final IMQBrokerPersist mqBrokerPersist = context.mqBrokerPersist();
                final IInvokeService invokeService = context.invokeService();
                final long responseTime = context.respTimeoutMills();
                final int pushMaxAttempt = context.pushMaxAttempt();
                
                final String topic = persistPut.getTopic();
                final int queueId = persistPut.getQueueId();
                

                // 更新状态为处理中
                final String messageId = mqMessage.getTraceId();
                log.info("开始更新消息为处理中：{}", messageId);

                for(final ChannelGroupNameDto channelGroupNameDto : channelList) {
                    final Channel channel = channelGroupNameDto.getChannel();
                    final String consumerGroupName =channelGroupNameDto.getConsumerGroupName();

                    try {
                        mqBrokerPersist.updateStatus(new MQConsumerUpdateStatusDto(messageId, MessageStatusConst.TO_CONSUMER_PROCESS,
                                consumerGroupName, topic, queueId, -1));

                        String channelId = ChannelUtil.getChannelId(channel);

                        log.info("开始处理 channelId: {}", channelId);
                        //1. 调用
                        mqMessage.setMethodType(MethodType.B_MESSAGE_PUSH);

                        // 重试推送
                        MQConsumerResultResp resultResp = Retryer.<MQConsumerResultResp>newInstance()
                                .maxAttempt(pushMaxAttempt)
                                .callable(new Callable<MQConsumerResultResp>() {
                                    @Override
                                    public MQConsumerResultResp call() throws Exception {
                                        MQConsumerResultResp resp = callServer(channel, mqMessage,
                                                MQConsumerResultResp.class, invokeService, responseTime);

                                        // 失败校验
                                        if(resp == null
                                                || !ConsumerStatus.SUCCESS.getCode()
                                                .equals(resp.getConsumerStatus())) {
                                            throw new MQException(BrokerRespCode.MSG_PUSH_FAILED);
                                        }
                                        return resp;
                                    }
                                }).retryCall();

                        //2. 更新状态
                        //2.1 处理成功，取 push 消费状态
                        if(MQCommonRespCode.SUCCESS.getCode().equals(resultResp.getRespCode())) {
                            mqBrokerPersist.updateStatus(new MQConsumerUpdateStatusDto(messageId, resultResp.getConsumerStatus(),
                                    consumerGroupName, topic, queueId, -1));
                        } else {
                            // 2.2 处理失败
                            log.error("消费失败：{}", JSON.toJSON(resultResp));
                            mqBrokerPersist.updateStatus(new MQConsumerUpdateStatusDto(messageId, MessageStatusConst.TO_CONSUMER_FAILED,
                                    consumerGroupName, topic, queueId, -1));
                        }
                        log.info("完成处理 channelId: {}", channelId);
                    } catch (Exception exception) {
                        log.error("处理异常");
                        mqBrokerPersist.updateStatus(new MQConsumerUpdateStatusDto(messageId, MessageStatusConst.TO_CONSUMER_FAILED,
                                consumerGroupName, topic, queueId, -1));
                    }
                }

                log.info("完成异步处理");
            }
        });
    }

    /**
     * 调用服务端
     * @param channel 调用通道
     * @param commonReq 通用请求
     * @param respClass 类
     * @param invokeService 调用管理类
     * @param respTimeoutMills 响应超时时间
     * @param <T> 泛型
     * @param <R> 结果
     * @return 结果
     */
    private <T extends MQCommonReq, R extends MQCommonResp> R callServer(Channel channel,
                                                                         T commonReq,
                                                                         Class<R> respClass,
                                                                         IInvokeService invokeService,
                                                                         long respTimeoutMills) {
        final String traceId = commonReq.getTraceId();
        final long requestTime = System.currentTimeMillis();

        RpcMessageDto rpcMessageDto = new RpcMessageDto();
        rpcMessageDto.setTraceId(traceId);
        rpcMessageDto.setRequestTime(requestTime);
        rpcMessageDto.setJson(JSON.toJSONString(commonReq));
        rpcMessageDto.setMethodType(commonReq.getMethodType());
        rpcMessageDto.setRequest(true);
        
        invokeService.addRequest(traceId, respTimeoutMills);
        ByteBuf byteBuf = DelimiterUtil.getMessageDelimiterBuffer(rpcMessageDto);
        channel.writeAndFlush(byteBuf);
        String channelId = ChannelUtil.getChannelId(channel);
        log.debug("[Client] channelId {} 发送消息 {}", channelId, JSON.toJSON(rpcMessageDto));
//        channel.closeFuture().syncUninterruptibly();

        if (respClass == null) {
            log.debug("[Client] 当前消息为 one-way 消息，忽略响应");
            return null;
        } else {
            //channelHandler 中获取对应的响应
            RpcMessageDto messageDto = invokeService.getResponse(traceId);
            if (MQCommonRespCode.TIMEOUT.getCode().equals(messageDto.getRespCode())) {
                throw new MQException(MQCommonRespCode.TIMEOUT);
            }

            String respJson = messageDto.getJson();
            return JSON.parseObject(respJson, respClass);
        }
    }
}
