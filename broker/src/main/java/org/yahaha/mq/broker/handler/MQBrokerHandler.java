package org.yahaha.mq.broker.handler;

import cn.hutool.core.collection.CollectionUtil;
import com.alibaba.fastjson.JSON;
import com.github.houbb.heaven.util.lang.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.api.IBrokerConsumerService;
import org.yahaha.mq.broker.api.IBrokerProducerService;
import org.yahaha.mq.broker.dto.BrokerRegisterReq;
import org.yahaha.mq.broker.dto.ChannelGroupNameDto;
import org.yahaha.mq.broker.dto.ServiceEntry;
import org.yahaha.mq.broker.dto.consumer.ConsumerSubscribeReq;
import org.yahaha.mq.broker.dto.consumer.ConsumerUnSubscribeReq;
import org.yahaha.mq.broker.dto.persist.MQMessagePersistPut;
import org.yahaha.mq.broker.persist.IMQBrokerPersist;
import org.yahaha.mq.broker.support.push.BrokerPushContext;
import org.yahaha.mq.broker.support.push.IBrokerPushService;
import org.yahaha.mq.common.constant.MethodType;
import org.yahaha.mq.common.dto.req.*;
import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.resp.MQCommonRespCode;
import org.yahaha.mq.common.rpc.RpcMessageDto;
import org.yahaha.mq.common.support.invoke.IInvokeService;
import org.yahaha.mq.common.util.ChannelUtil;
import org.yahaha.mq.common.util.DelimiterUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class MQBrokerHandler extends SimpleChannelInboundHandler {

    /**
     * 调用管理类
     */
    private IInvokeService invokeService;

    /**
     * 消费者管理
     */
    private IBrokerConsumerService registerConsumerService;

    /**
     * 生产者管理
     */
    private IBrokerProducerService registerProducerService;

    /**
     * 持久化类
     */
    private IMQBrokerPersist mqBrokerPersist;

    /**
     * 推送服务
     */
    private IBrokerPushService brokerPushService;

    /**
     * 获取响应超时时间
     */
    private long respTimeoutMills;

    /**
     * 推送最大尝试次数
     */
    private int pushMaxAttempt;

    /**
     * 新建消费队列数量
     */
    private int newConsumeQueueNum;

    public MQBrokerHandler newConsumeQueueNum(int newConsumeQueueNum) {
        this.newConsumeQueueNum = newConsumeQueueNum;
        return this;
    }

    public MQBrokerHandler invokeService(IInvokeService invokeService) {
        this.invokeService = invokeService;
        return this;
    }

    public MQBrokerHandler registerConsumerService(IBrokerConsumerService registerConsumerService) {
        this.registerConsumerService = registerConsumerService;
        return this;
    }

    public MQBrokerHandler registerProducerService(IBrokerProducerService registerProducerService) {
        this.registerProducerService = registerProducerService;
        return this;
    }

    public MQBrokerHandler mqBrokerPersist(IMQBrokerPersist mqBrokerPersist) {
        this.mqBrokerPersist = mqBrokerPersist;
        return this;
    }

    public MQBrokerHandler brokerPushService(IBrokerPushService brokerPushService) {
        this.brokerPushService = brokerPushService;
        return this;
    }

    public MQBrokerHandler respTimeoutMills(long respTimeoutMills) {
        this.respTimeoutMills = respTimeoutMills;
        return this;
    }

    public MQBrokerHandler pushMaxAttempt(int pushMaxAttempt) {
        this.pushMaxAttempt = pushMaxAttempt;
        return this;
    }


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);

        RpcMessageDto rpcMessageDto = null;
        try {
            rpcMessageDto = JSON.parseObject(bytes, RpcMessageDto.class);
        } catch (Exception exception) {
            log.error("RpcMessageDto json 格式转换异常 {}", new String(bytes));
            return;
        }

        if (rpcMessageDto.isRequest()) {
            MQCommonResp commonResp = this.dispatch(rpcMessageDto, ctx);

            if(commonResp == null) {
                log.debug("当前消息为 null，忽略处理。");
                return;
            }

            // 写回响应，和以前类似。
            writeResponse(rpcMessageDto, commonResp, ctx);
        } else {
            final String traceId = rpcMessageDto.getTraceId();

            // 丢弃掉 traceId 为空的信息
            if(StringUtil.isBlank(traceId)) {
                log.debug("[Server Response] response traceId 为空，直接丢弃", JSON.toJSON(rpcMessageDto));
                return;
            }

            // 添加消息
            invokeService.addResponse(traceId, rpcMessageDto);
        }
    }

    /**
     * 结果写回
     *
     * @param req  请求
     * @param resp 响应
     * @param ctx  上下文
     */
    private void writeResponse(RpcMessageDto req, Object resp, ChannelHandlerContext ctx) {
        final String id = ctx.channel().id().asLongText();
        RpcMessageDto rpcMessageDto = new RpcMessageDto();
        // 响应类消息
        rpcMessageDto.setRequest(false);
        rpcMessageDto.setTraceId(req.getTraceId());
        rpcMessageDto.setMethodType(req.getMethodType());
        rpcMessageDto.setRequestTime(System.currentTimeMillis());
        String json = JSON.toJSONString(resp);
        rpcMessageDto.setJson(json);
        // 回写到 client 端
        ByteBuf byteBuf = DelimiterUtil.getMessageDelimiterBuffer(rpcMessageDto);
        ctx.writeAndFlush(byteBuf);
        log.debug("[Server] channel {} response {}", id, JSON.toJSON(rpcMessageDto));
    }

    /**
     * 消息的分发
     *
     * @param rpcMessageDto 入参
     * @param ctx 上下文
     * @return 结果
     */
    private MQCommonResp dispatch(RpcMessageDto rpcMessageDto, ChannelHandlerContext ctx) {
        try {
            final String methodType = rpcMessageDto.getMethodType();
            final String json = rpcMessageDto.getJson();
            String channelId = ChannelUtil.getChannelId(ctx);
            final Channel channel = ctx.channel();
            log.debug("channelId: {} 接收到 method: {} 内容：{}", channelId,
                    methodType, json);

            // 生产者注册
            if(MethodType.P_REGISTER.equals(methodType)) {
                BrokerRegisterReq registerReq = JSON.parseObject(json, BrokerRegisterReq.class);
                return registerProducerService.register(registerReq.getServiceEntry(), channel);
            }
            // 生产者注销
            if(MethodType.P_UN_REGISTER.equals(methodType)) {
                BrokerRegisterReq registerReq = JSON.parseObject(json, BrokerRegisterReq.class);
                return registerProducerService.unRegister(registerReq.getServiceEntry(), channel);
            }
            // 生产者消息发送
            if(MethodType.P_SEND_MSG.equals(methodType)) {
                return handleProducerSendMsg(channelId, json);
            }
            // 生产者消息发送-ONE WAY
            if(MethodType.P_SEND_MSG_ONE_WAY.equals(methodType)) {
                handleProducerSendMsg(channelId, json);
                return null;
            }

            // 消费者注册
            if(MethodType.C_REGISTER.equals(methodType)) {
                BrokerRegisterReq registerReq = JSON.parseObject(json, BrokerRegisterReq.class);
                return registerConsumerService.register(registerReq.getServiceEntry(), channel);
            }
            // 消费者注销
            if(MethodType.C_UN_REGISTER.equals(methodType)) {
                BrokerRegisterReq registerReq = JSON.parseObject(json, BrokerRegisterReq.class);
                return registerConsumerService.unRegister(registerReq.getServiceEntry(), channel);
            }
            // 消费者监听注册
            if(MethodType.C_SUBSCRIBE.equals(methodType)) {
                ConsumerSubscribeReq req = JSON.parseObject(json, ConsumerSubscribeReq.class);
                return registerConsumerService.subscribe(req, channel);
            }
            // 消费者监听注销
            if(MethodType.C_UN_SUBSCRIBE.equals(methodType)) {
                ConsumerUnSubscribeReq req = JSON.parseObject(json, ConsumerUnSubscribeReq.class);
                return registerConsumerService.unSubscribe(req, channel);
            }

            // 消费者心跳
            if(MethodType.C_HEARTBEAT.equals(methodType)) {
                MQHeartBeatReq req = JSON.parseObject(json, MQHeartBeatReq.class);
                registerConsumerService.heartbeat(req, channel);
                return null;
            }

            // 消费者主动 pull
            if(MethodType.C_MESSAGE_PULL.equals(methodType)) {
                MQPullConsumerReq req = JSON.parseObject(json, MQPullConsumerReq.class);
                return mqBrokerPersist.pull(req, channel);
            }

            // 消费者消费状态 ACK
            if(MethodType.C_CONSUMER_STATUS.equals(methodType)) {
                MQConsumerUpdateStatusReq req = JSON.parseObject(json, MQConsumerUpdateStatusReq.class);
                MQConsumerUpdateStatusDto statusDto = req.getConsumerUpdateStatusDto();
                return mqBrokerPersist.updateStatus(statusDto);
            }

            //消费者消费状态 ACK-批量
            if(MethodType.C_CONSUMER_STATUS_BATCH.equals(methodType)) {
//              registerConsumerService.checkValid(channelId);

                MQConsumerUpdateStatusBatchReq req = JSON.parseObject(json, MQConsumerUpdateStatusBatchReq.class);
                final List<MQConsumerUpdateStatusDto> statusDtoList = req.getStatusList();
                return mqBrokerPersist.updateStatusBatch(statusDtoList);
            }

            // 生产者消息发送-批量
            if(MethodType.P_SEND_MSG_BATCH.equals(methodType)) {
                return handleProducerSendMsgBatch(channelId, json);
            }

            // 生产者消息发送-ONE WAY-批量
            if(MethodType.P_SEND_MSG_ONE_WAY_BATCH.equals(methodType)) {
                handleProducerSendMsgBatch(channelId, json);
                return null;
            }
            throw new UnsupportedOperationException("暂不支持的方法类型");
        } catch (Exception exception) {
            log.error("执行异常", exception);
            MQCommonResp resp = new MQCommonResp();
            resp.setRespCode(MQCommonRespCode.FAIL.getCode());
            resp.setRespMessage(MQCommonRespCode.FAIL.getMsg());
            return resp;
        }
    }

    /**
     *  处理生产者发送的消息
     *
     * @param json 消息体
     */
    private MQCommonResp handleProducerSendMsg(String channelId, String json) {
        MQMessage mqMessage = JSON.parseObject(json, MQMessage.class);
        final ServiceEntry serviceEntry = registerProducerService.getServiceEntry(channelId);
        MQMessagePersistPut persistPut = buildPersistPut(mqMessage, serviceEntry);

        MQCommonResp commonResp = mqBrokerPersist.putMessage(persistPut);
//        this.asyncHandleMessage(persistPut);
        return commonResp;
    }

    /**
     * 处理生产者发送的消息
     *
     * @param channelId 通道标识
     * @param json 消息体
     */
    private MQCommonResp handleProducerSendMsgBatch(String channelId, String json) {
        MQMessageBatchReq batchReq = JSON.parseObject(json, MQMessageBatchReq.class);
        final ServiceEntry serviceEntry = registerProducerService.getServiceEntry(channelId);
        List<MQMessagePersistPut> putList = buildPersistPutList(batchReq, serviceEntry);

        MQCommonResp commonResp = mqBrokerPersist.putMessageBatch(putList);

        // 遍历异步推送
//        for(MQMessagePersistPut persistPut : putList) {
//            this.asyncHandleMessage(persistPut);
//        }
        return commonResp;
    }

    /**
     * 构建持久化对象
     *
     * @param mqMessage 消息
     * @param serviceEntry 实例
     * @return 结果
     */
    private MQMessagePersistPut buildPersistPut(MQMessage mqMessage, ServiceEntry serviceEntry) {
        int queueNum = mqBrokerPersist.getConsumeQueueNum(mqMessage.getTopic());
        int queueID = ThreadLocalRandom.current().nextInt(0, queueNum);
        MQMessagePersistPut persistPut = new MQMessagePersistPut();
        persistPut.setMqMessage(mqMessage);
        persistPut.setTopic(mqMessage.getTopic());
        persistPut.setQueueId(queueID);
        persistPut.setRpcAddress(serviceEntry);
        return persistPut;
    }

    /**
     * 构建列表
     *
     * @param batchReq 入参
     * @param serviceEntry 实例
     * @return 结果
     */
    private List<MQMessagePersistPut> buildPersistPutList(MQMessageBatchReq batchReq,
                                                          final ServiceEntry serviceEntry) {
        List<MQMessagePersistPut> resultList = new ArrayList<>();

        // 构建列表
        List<MQMessage> messageList = batchReq.getMqMessageList();
        for(MQMessage mqMessage : messageList) {
            MQMessagePersistPut put = buildPersistPut(mqMessage, serviceEntry);

            resultList.add(put);
        }

        return resultList;
    }

    /**
     * 异步处理消息
     */
    private void asyncHandleMessage(MQMessagePersistPut put) {
        final MQMessage mqMessage = put.getMqMessage();
        List<ChannelGroupNameDto> channelList = registerConsumerService.getPushSubscribeList(mqMessage);
        if(CollectionUtil.isEmpty(channelList)) {
            log.info("监听列表为空，忽略处理");
            return;
        }

        BrokerPushContext brokerPushContext = BrokerPushContext.newInstance()
                .channelList(channelList)
                .mqMessagePersistPut(put)
                .mqBrokerPersist(mqBrokerPersist)
                .invokeService(invokeService)
                .respTimeoutMills(respTimeoutMills)
                .pushMaxAttempt(pushMaxAttempt);

        brokerPushService.asyncPush(brokerPushContext);
    }
}
