package org.yahaha.mq.producer.support.broker;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSON;
import com.github.houbb.sisyphus.core.core.Retryer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.dto.BrokerRegisterReq;
import org.yahaha.mq.broker.dto.ServiceEntry;
import org.yahaha.mq.broker.util.InnerChannelUtil;
import org.yahaha.mq.common.constant.MethodType;
import org.yahaha.mq.common.dto.req.MQCommonReq;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.req.MQMessageBatchReq;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.loadbalance.api.ILoadBalance;
import org.yahaha.mq.common.resp.MQCommonRespCode;
import org.yahaha.mq.common.resp.MQException;
import org.yahaha.mq.common.rpc.RpcChannelFuture;
import org.yahaha.mq.common.rpc.RpcMessageDto;
import org.yahaha.mq.common.support.invoke.IInvokeService;
import org.yahaha.mq.common.support.status.IStatusManager;
import org.yahaha.mq.common.util.*;
import org.yahaha.mq.producer.constant.ProducerRespCode;
import org.yahaha.mq.producer.constant.SendStatus;
import org.yahaha.mq.producer.dto.SendBatchResult;
import org.yahaha.mq.producer.dto.SendResult;
import org.yahaha.mq.producer.handler.MQProducerHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Slf4j
public class ProducerBrokerService implements IProducerBrokerService{

    /**
     * 分组名称
     */
    private String groupName;

    /**
     * 中间人地址
     */
    private String brokerAddress;

    /**
     * 调用管理服务
     *      */
    private IInvokeService invokeService;

    /**
     * 获取响应超时时间
     *      */
    private long respTimeoutMills;

    /**
     * 请求列表
     *      */
    private List<RpcChannelFuture> channelFutureList;

    /**
     * 检测 broker 可用性
     *      */
    private boolean check;

    /**
     * 状态管理
     *      */
    private IStatusManager statusManager;

    /**
     * 负载均衡策略
     */
    private ILoadBalance<RpcChannelFuture> loadBalance;

    /**
     * 消息发送最大尝试次数
     */
    private int maxAttempt = 3;

    @Override
    public void initChannelFutureList(ProducerBrokerConfig config) {
        //1. 配置初始化
        this.invokeService = config.invokeService();
        this.check = config.check();
        this.respTimeoutMills = config.respTimeoutMills();
        this.brokerAddress = config.brokerAddress();
        this.groupName = config.groupName();
        this.statusManager = config.statusManager();
        this.loadBalance = config.loadBalance();

        //2. 初始化
        this.channelFutureList = ChannelFutureUtil.initChannelFutureList(brokerAddress,
                initChannelHandler());
    }

    private ChannelHandler initChannelHandler() {
        final ByteBuf delimiterBuf = DelimiterUtil.getByteBuf(DelimiterUtil.DELIMITER);

        final MQProducerHandler mqProducerHandler = new MQProducerHandler();
        mqProducerHandler.setInvokeService(invokeService);

        // handler 实际上会被多次调用，如果不是 @Shareable，应该每次都重新创建。
        ChannelHandler handler = new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.pipeline()
                        .addLast(new DelimiterBasedFrameDecoder(DelimiterUtil.LENGTH, delimiterBuf))
                        .addLast(mqProducerHandler);
            }
        };

        return handler;
    }

    @Override
    public void registerToBroker() {
        for(RpcChannelFuture channelFuture : this.channelFutureList) {
            ServiceEntry serviceEntry = new ServiceEntry();
            serviceEntry.setGroupName(groupName);
            serviceEntry.setAddress(channelFuture.getAddress());
            serviceEntry.setPort(channelFuture.getPort());
            serviceEntry.setWeight(channelFuture.getWeight());

            BrokerRegisterReq brokerRegisterReq = new BrokerRegisterReq();
            brokerRegisterReq.setServiceEntry(serviceEntry);
            brokerRegisterReq.setMethodType(MethodType.P_REGISTER);
            brokerRegisterReq.setTraceId(IdUtil.simpleUUID());

            log.info("[Register] 开始注册到 broker：{}", JSON.toJSON(brokerRegisterReq));
            final Channel channel = channelFuture.getChannelFuture().channel();
            MQCommonResp resp = callServer(channel, brokerRegisterReq, MQCommonResp.class);
            log.info("[Register] 完成注册到 broker：{}", JSON.toJSON(resp));
        }
    }

    @Override
    public <T extends MQCommonReq, R extends MQCommonResp> R callServer(Channel channel, T commonReq, Class<R> respClass) {
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

    @Override
    public Channel getChannel(String key) {
        // 等待启动完成
        while (!statusManager.status()) {
            log.debug("等待初始化完成...");
            SleepUtil.sleep(100);
        }

        RpcChannelFuture rpcChannelFuture = LoadBalanceUtil.loadBalance(this.loadBalance,
                channelFutureList, key);
        return rpcChannelFuture.getChannelFuture().channel();
    }

    @Override
    public SendResult send(MQMessage mqMessage) {
        String messageId = IdUtil.simpleUUID();
        mqMessage.setTraceId(messageId);
        mqMessage.setMethodType(MethodType.P_SEND_MSG);
        mqMessage.setGroupName(groupName);

        return Retryer.<SendResult>newInstance()
                .maxAttempt(maxAttempt)
                .callable(new Callable<SendResult>() {
                    @Override
                    public SendResult call() throws Exception {
                        return doSend(messageId, mqMessage);
                    }
                }).retryCall();
    }

    private SendResult doSend(String messageId, MQMessage mqMessage) {
        log.info("[Producer] 发送消息 messageId: {}, mqMessage: {}",
                messageId, JSON.toJSON(mqMessage));

        Channel channel = getChannel(mqMessage.getShardingKey());
        MQCommonResp resp = callServer(channel, mqMessage, MQCommonResp.class);
        if(MQCommonRespCode.SUCCESS.getCode().equals(resp.getRespCode())) {
            return SendResult.of(messageId, SendStatus.SUCCESS);
        }

        throw new MQException(ProducerRespCode.MSG_SEND_FAILED);
    }

    @Override
    public SendResult sendOneWay(MQMessage mqMessage) {
        String messageId = IdUtil.simpleUUID();
        mqMessage.setTraceId(messageId);
        mqMessage.setMethodType(MethodType.P_SEND_MSG_ONE_WAY);
        mqMessage.setGroupName(groupName);


        Channel channel = getChannel(mqMessage.getShardingKey());
        this.callServer(channel, mqMessage, null);

        return SendResult.of(messageId, SendStatus.SUCCESS);
    }

    @Override
    public SendBatchResult sendBatch(List<MQMessage> mqMessageList) {
        final List<String> messageIdList = this.fillMessageList(mqMessageList);
        final MQMessageBatchReq batchReq = new MQMessageBatchReq();
        batchReq.setMqMessageList(mqMessageList);
        String traceId = IdUtil.simpleUUID();
        batchReq.setTraceId(traceId);
        batchReq.setMethodType(MethodType.P_SEND_MSG_BATCH);
        return Retryer.<SendBatchResult>newInstance()
                .maxAttempt(maxAttempt)
                .callable(new Callable<SendBatchResult>() {
                    @Override
                    public SendBatchResult call() throws Exception {
                        return doSendBatch(messageIdList, batchReq, false);
                    }
                }).retryCall();
    }

    @Override
    public SendBatchResult sendOneWayBatch(List<MQMessage> mqMessageList) {
        List<String> messageIdList = this.fillMessageList(mqMessageList);
        MQMessageBatchReq batchReq = new MQMessageBatchReq();
        batchReq.setMqMessageList(mqMessageList);
        String traceId = IdUtil.simpleUUID();
        batchReq.setTraceId(traceId);
        batchReq.setMethodType(MethodType.P_SEND_MSG_ONE_WAY_BATCH);
        return doSendBatch(messageIdList, batchReq, true);
    }


    private SendBatchResult doSendBatch(List<String> messageIdList,
                                        MQMessageBatchReq batchReq,
                                        boolean oneWay) {
        log.info("[Producer] 批量发送消息 messageIdList: {}, batchReq: {}, oneWay: {}",
                messageIdList, JSON.toJSON(batchReq), oneWay);
        // 以第一个 sharding-key 为准。
        // 后续的会被忽略
        MQMessage mqMessage = batchReq.getMqMessageList().get(0);
        Channel channel = getChannel(mqMessage.getShardingKey());
        //one-way
        if(oneWay) {
            log.warn("[Producer] ONE-WAY send, ignore result");
            return SendBatchResult.of(messageIdList, SendStatus.SUCCESS);
        }
        MQCommonResp resp = callServer(channel, batchReq, MQCommonResp.class);
        if(MQCommonRespCode.SUCCESS.getCode().equals(resp.getRespCode())) {
            return SendBatchResult.of(messageIdList, SendStatus.SUCCESS);
        }
        throw new MQException(ProducerRespCode.MSG_SEND_FAILED);
    }

    private List<String> fillMessageList(final List<MQMessage> mqMessageList) {
        List<String> idList = new ArrayList<>(mqMessageList.size());

        for(MQMessage mqMessage : mqMessageList) {
            String messageId = IdUtil.simpleUUID();
            mqMessage.setTraceId(messageId);
            mqMessage.setGroupName(groupName);

            idList.add(messageId);
        }

        return idList;
    }

    @Override
    public void destroyAll() {
        for(RpcChannelFuture channelFuture : channelFutureList) {
            Channel channel = channelFuture.getChannelFuture().channel();
            final String channelId = ChannelUtil.getChannelId(channel);
            log.info("开始注销：{}", channelId);

            ServiceEntry serviceEntry = InnerChannelUtil.buildServiceEntry(channelFuture);

            BrokerRegisterReq brokerRegisterReq = new BrokerRegisterReq();
            brokerRegisterReq.setServiceEntry(serviceEntry);

            String messageId = IdUtil.simpleUUID();
            brokerRegisterReq.setTraceId(messageId);
            brokerRegisterReq.setMethodType(MethodType.P_UN_REGISTER);

            this.callServer(channel, brokerRegisterReq, null);

            log.info("完成注销：{}", channelId);
        }
    }

}
