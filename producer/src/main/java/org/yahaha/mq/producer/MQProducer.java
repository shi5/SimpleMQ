package org.yahaha.mq.producer;

import com.github.houbb.heaven.util.common.ArgUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.loadbalance.LoadBalances;
import org.yahaha.mq.common.loadbalance.api.ILoadBalance;
import org.yahaha.mq.common.resp.MQException;
import org.yahaha.mq.common.rpc.RpcChannelFuture;
import org.yahaha.mq.common.support.hook.DefaultShutdownHook;
import org.yahaha.mq.common.support.hook.ShutdownHooks;
import org.yahaha.mq.common.support.invoke.IInvokeService;
import org.yahaha.mq.common.support.invoke.InvokeService;
import org.yahaha.mq.common.support.status.IStatusManager;
import org.yahaha.mq.common.support.status.StatusManager;
import org.yahaha.mq.common.util.DelimiterUtil;
import org.yahaha.mq.producer.api.IMQProducer;
import org.yahaha.mq.producer.constant.ProducerConst;
import org.yahaha.mq.producer.constant.ProducerRespCode;
import org.yahaha.mq.producer.dto.SendBatchResult;
import org.yahaha.mq.producer.dto.SendResult;
import org.yahaha.mq.producer.handler.MQProducerHandler;
import org.yahaha.mq.producer.support.broker.IProducerBrokerService;
import org.yahaha.mq.producer.support.broker.ProducerBrokerConfig;
import org.yahaha.mq.producer.support.broker.ProducerBrokerService;

import java.util.List;

@Slf4j
@Data
public class MQProducer extends Thread implements IMQProducer {

    /**
     * 分组名称
     */
    private String groupName = ProducerConst.DEFAULT_GROUP_NAME;

    /**
     * 中间人地址
     */
    private String brokerAddress  = "127.0.0.1:9999";

    /**
     * 调用管理服务
     */
    private IInvokeService invokeService = new InvokeService();

    /**
     * 生产者-中间服务端服务类
     */
    private IProducerBrokerService producerBrokerService = new ProducerBrokerService();

    /**
     * 检测 broker 可用性
     */
    private volatile boolean check = true;

    /**
     * 获取响应超时时间
     */
    private long respTimeoutMills = 5000;

    /**
     * 状态管理类
     */
    private IStatusManager statusManager = new StatusManager();

    /**
     * 请求列表
     */
    private List<RpcChannelFuture> channelFutureList;

    /**
     * 优雅关闭等待时间
     */
    private long waitMillsForRemainRequest = 60 * 1000;

    /**
     * 消息发送最大尝试次数
     */
    private int maxAttempt = 3;

    /**
     * 负载均衡策略
     */
    private ILoadBalance<RpcChannelFuture> loadBalance = LoadBalances.weightedRoundRobin();

    public MQProducer groupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public MQProducer brokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
        return this;
    }

    public MQProducer respTimeoutMills(long respTimeoutMills) {
        this.respTimeoutMills = respTimeoutMills;
        return this;
    }

    public MQProducer check(boolean check) {
        this.check = check;
        return this;
    }

    public MQProducer waitMillsForRemainRequest(long waitMillsForRemainRequest) {
        this.waitMillsForRemainRequest = waitMillsForRemainRequest;
        return this;
    }

    public MQProducer loadBalance(ILoadBalance<RpcChannelFuture> loadBalance) {
        this.loadBalance = loadBalance;
        return this;
    }

    public MQProducer maxAttempt(int maxAttempt) {
        this.maxAttempt = maxAttempt;
        return this;
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

    /**
     * 参数校验
     */
    private void paramCheck() {
        ArgUtil.notEmpty(brokerAddress, "brokerAddress");
        ArgUtil.notEmpty(groupName, "groupName");
    }

    @Override
    public synchronized void run() {
        this.paramCheck();

        // 启动服务端
        log.info("MQ 生产者开始启动客户端 GROUP: {}, brokerAddress: {}",
                groupName, brokerAddress);

        try {
            ProducerBrokerConfig config = ProducerBrokerConfig.newInstance()
                    .groupName(groupName)
                    .brokerAddress(brokerAddress)
                    .check(true)
                    .respTimeoutMills(respTimeoutMills)
                    .invokeService(invokeService)
                    .statusManager(statusManager)
                    .loadBalance(loadBalance);

            //1. 初始化
            this.producerBrokerService.initChannelFutureList(config);
            //2. 连接到服务端
            this.producerBrokerService.registerToBroker();

            //3. 标识为可用
            statusManager.status(true);

            //4. 添加钩子函数
            final DefaultShutdownHook rpcShutdownHook = new DefaultShutdownHook();
            rpcShutdownHook.setStatusManager(statusManager);
            rpcShutdownHook.setInvokeService(invokeService);
            rpcShutdownHook.setWaitMillsForRemainRequest(waitMillsForRemainRequest);
            rpcShutdownHook.setDestroyable(this.producerBrokerService);
            ShutdownHooks.rpcShutdownHook(rpcShutdownHook);
        } catch (Exception e) {
            log.error("MQ 生产者启动遇到异常", e);
            throw new MQException(ProducerRespCode.RPC_INIT_FAILED);
        }
    }

    @Override
    public SendResult send(MQMessage mqMessage) {
        return this.producerBrokerService.send(mqMessage);
    }

    @Override
    public SendResult sendOneWay(MQMessage mqMessage) {
        return this.producerBrokerService.sendOneWay(mqMessage);
    }

    @Override
    public SendBatchResult sendBatch(List<MQMessage> mqMessageList) {
        return producerBrokerService.sendBatch(mqMessageList);
    }

    @Override
    public SendBatchResult sendOneWayBatch(List<MQMessage> mqMessageList) {
        return producerBrokerService.sendOneWayBatch(mqMessageList);
    }
}
