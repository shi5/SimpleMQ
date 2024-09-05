package org.yahaha.mq.consumer.support.broker;

import cn.hutool.core.util.IdUtil;
import com.alibaba.fastjson.JSON;
import com.github.houbb.heaven.util.net.NetUtil;
import com.github.houbb.sisyphus.core.core.Retryer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.dto.BrokerRegisterReq;
import org.yahaha.mq.broker.dto.ServiceEntry;
import org.yahaha.mq.broker.dto.consumer.ConsumerSubscribeReq;
import org.yahaha.mq.broker.dto.consumer.ConsumerUnSubscribeReq;
import org.yahaha.mq.broker.util.InnerChannelUtil;
import org.yahaha.mq.common.constant.MethodType;
import org.yahaha.mq.common.dto.req.*;
import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.dto.resp.MQPullConsumerResp;
import org.yahaha.mq.common.loadbalance.api.ILoadBalance;
import org.yahaha.mq.common.resp.MQCommonRespCode;
import org.yahaha.mq.common.resp.MQException;
import org.yahaha.mq.common.rpc.RpcChannelFuture;
import org.yahaha.mq.common.rpc.RpcMessageDto;
import org.yahaha.mq.common.support.invoke.IInvokeService;
import org.yahaha.mq.common.support.status.IStatusManager;
import org.yahaha.mq.common.util.*;
import org.yahaha.mq.consumer.constant.ConsumerRespCode;
import org.yahaha.mq.consumer.handler.MQConsumerHandler;
import org.yahaha.mq.consumer.support.listener.IMQListenerService;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ConsumerBrokerService implements IConsumerBrokerService {
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
     */
    private IInvokeService invokeService;

    /**
     * 获取响应超时时间
     */
    private long respTimeoutMills;

    /**
     * 请求列表
     */
    private List<RpcChannelFuture> channelFutureList;

    /**
     * 检测 broker 可用性
     */
    private boolean check;

    /**
     * 状态管理
     */
    private IStatusManager statusManager;

    /**
     * 监听服务类
     */
    private IMQListenerService mqListenerService;

    /**
     * 心跳定时任务
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    /**
     * 负载均衡策略
     */
    private ILoadBalance<RpcChannelFuture> loadBalance;

    /**
     * 订阅最大尝试次数
     */
    private int subscribeMaxAttempt;

    /**
     * 取消订阅最大尝试次数
     */
    private int unSubscribeMaxAttempt;

    /**
     * 消费状态更新最大尝试次数
     */
    private int consumerStatusMaxAttempt;
    
    /**
     * 消费者最大尝试次数
     */
    private int consumeMaxAttempt;

    @Override
    public void initChannelFutureList(ConsumerBrokerConfig config) {
        //1. 配置初始化
        this.invokeService = config.invokeService();
        this.check = config.check();
        this.respTimeoutMills = config.respTimeoutMills();
        this.brokerAddress = config.brokerAddress();
        this.groupName = config.groupName();
        this.statusManager = config.statusManager();
        this.mqListenerService = config.mqListenerService();
        this.loadBalance = config.loadBalance();
        this.subscribeMaxAttempt = config.subscribeMaxAttempt();
        this.unSubscribeMaxAttempt = config.unSubscribeMaxAttempt();
        this.consumerStatusMaxAttempt = config.consumerStatusMaxAttempt();

        //2. 初始化
        this.channelFutureList = ChannelFutureUtil.initChannelFutureList(brokerAddress,
                initChannelHandler());

        //3. 初始化心跳任务
        initHeartbeat();
    }

    private ChannelHandler initChannelHandler() {
        final ByteBuf delimiterBuf = DelimiterUtil.getByteBuf(DelimiterUtil.DELIMITER);

        final MQConsumerHandler mqConsumerHandler = new MQConsumerHandler(invokeService, mqListenerService);

        // handler 实际上会被多次调用，如果不是 @Shareable，应该每次都重新创建。
        ChannelHandler handler = new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                ch.pipeline()
                        .addLast(new DelimiterBasedFrameDecoder(DelimiterUtil.LENGTH, delimiterBuf))
                        .addLast(mqConsumerHandler);
            }
        };

        return handler;
    }

    /**
     * 初始化心跳
     */
    private void initHeartbeat() {
        //5S 发一次心跳
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                heartbeat();
            }
        }, 5, 5, TimeUnit.SECONDS);
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
            brokerRegisterReq.setMethodType(MethodType.C_REGISTER);
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

        // 需要响应的请求，添加调用服务
        if (respClass != null) invokeService.addRequest(traceId, respTimeoutMills);

        // 遍历 channel
        // 关闭当前线程，以获取对应的信息
        // 使用序列化的方式
        ByteBuf byteBuf = DelimiterUtil.getMessageDelimiterBuffer(rpcMessageDto);

        //负载均衡获取 channel
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

        RpcChannelFuture rpcChannelFuture = LoadBalanceUtil.loadBalance(loadBalance,
                channelFutureList, key);
        return rpcChannelFuture.getChannelFuture().channel();
    }

    @Override
    // TODO: 是否应该向所有的Broker进行注册
    public void subscribe(String topicName, String tagRegex) {
        ConsumerSubscribeReq req = new ConsumerSubscribeReq();

        String messageId = IdUtil.simpleUUID();
        req.setTraceId(messageId);
        req.setMethodType(MethodType.C_SUBSCRIBE);
        req.setTopicName(topicName);
        req.setTagRegex(tagRegex);
        req.setGroupName(groupName);

        // 重试订阅
        Retryer.<String>newInstance()
                .maxAttempt(subscribeMaxAttempt)
                .callable(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        Channel channel = getChannel(null);
                        MQCommonResp resp = callServer(channel, req, MQCommonResp.class);
                        if(!MQCommonRespCode.SUCCESS.getCode().equals(resp.getRespCode())) {
                            throw new MQException(ConsumerRespCode.SUBSCRIBE_FAILED);
                        }
                        return resp.getRespCode();
                    }
                }).retryCall();
    }

    @Override
    public void unSubscribe(String topicName, String tagRegex) {
        ConsumerUnSubscribeReq req = new ConsumerUnSubscribeReq();

        String messageId = IdUtil.simpleUUID();
        req.setTraceId(messageId);
        req.setMethodType(MethodType.C_UN_SUBSCRIBE);
        req.setTopicName(topicName);
        req.setTagRegex(tagRegex);
        req.setGroupName(groupName);

        // 重试取消订阅
        Retryer.<String>newInstance()
                .maxAttempt(unSubscribeMaxAttempt)
                .callable(new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        Channel channel = getChannel(null);
                        MQCommonResp resp = callServer(channel, req, MQCommonResp.class);
                        if(!MQCommonRespCode.SUCCESS.getCode().equals(resp.getRespCode())) {
                            throw new MQException(ConsumerRespCode.UN_SUBSCRIBE_FAILED);
                        }
                        return resp.getRespCode();
                    }
                }).retryCall();
    }

    @Override
    public void heartbeat() {
        final MQHeartBeatReq req = new MQHeartBeatReq();
        final String traceId = IdUtil.simpleUUID();
        req.setTraceId(traceId);
        req.setMethodType(MethodType.C_HEARTBEAT);
        req.setAddress(NetUtil.getLocalHost());
        req.setPort(0);
        req.setTime(System.currentTimeMillis());

        log.debug("[HEARTBEAT] 往服务端发送心跳包 {}", JSON.toJSON(req));

        // 通知全部
        for(RpcChannelFuture channelFuture : channelFutureList) {
            try {
                Channel channel = channelFuture.getChannelFuture().channel();
                callServer(channel, req, null);
            } catch (Exception exception) {
                log.error("[HEARTBEAT] 往服务端处理异常", exception);
            }
        }
    }

    @Override
    public MQPullConsumerResp pull(String topicName, String tagRegex, int fetchSize) {
        MQPullConsumerReq req = new MQPullConsumerReq();
        req.setSize(fetchSize);
        req.setGroupName(groupName);
        req.setTagRegex(tagRegex);
        req.setTopic(topicName);
        final String traceId = IdUtil.simpleUUID();
        req.setTraceId(traceId);
        req.setMethodType(MethodType.C_MESSAGE_PULL);

        Channel channel = getChannel(null);
        return this.callServer(channel, req, MQPullConsumerResp.class);
    }

    @Override
    public MQCommonResp consumerStatusAck(MQConsumerUpdateStatusDto statusDto) {
        final MQConsumerUpdateStatusReq req = new MQConsumerUpdateStatusReq(statusDto);
        
        final String traceId = IdUtil.simpleUUID();
        req.setTraceId(traceId);
        req.setMethodType(MethodType.C_CONSUMER_STATUS);

        // 重试
        return Retryer.<MQCommonResp>newInstance()
                .maxAttempt(consumerStatusMaxAttempt)
                .callable(new Callable<MQCommonResp>() {
                    @Override
                    public MQCommonResp call() throws Exception {
                        Channel channel = getChannel(null);
                        MQCommonResp resp = callServer(channel, req, MQCommonResp.class);
                        if(!MQCommonRespCode.SUCCESS.getCode().equals(resp.getRespCode())) {
                            throw new MQException(ConsumerRespCode.CONSUMER_STATUS_ACK_FAILED);
                        }
                        return resp;
                    }
                }).retryCall();
    }

    @Override
    public MQCommonResp consumerStatusAckBatch(List<MQConsumerUpdateStatusDto> statusDtoList) {
        final MQConsumerUpdateStatusBatchReq req = new MQConsumerUpdateStatusBatchReq();
        req.setStatusList(statusDtoList);

        final String traceId = IdUtil.simpleUUID();
        req.setTraceId(traceId);
        req.setMethodType(MethodType.C_CONSUMER_STATUS_BATCH);

        // 重试
        return Retryer.<MQCommonResp>newInstance()
                .maxAttempt(consumerStatusMaxAttempt)
                .callable(new Callable<MQCommonResp>() {
                    @Override
                    public MQCommonResp call() throws Exception {
                        Channel channel = getChannel(null);
                        MQCommonResp resp = callServer(channel, req, MQCommonResp.class);
                        if(!MQCommonRespCode.SUCCESS.getCode().equals(resp.getRespCode())) {
                            throw new MQException(ConsumerRespCode.CONSUMER_STATUS_ACK_BATCH_FAILED);
                        }
                        return resp;
                    }
                }).retryCall();
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
            brokerRegisterReq.setMethodType(MethodType.C_UN_REGISTER);

            this.callServer(channel, brokerRegisterReq, null);

            log.info("完成注销：{}", channelId);
        }
    }


}
