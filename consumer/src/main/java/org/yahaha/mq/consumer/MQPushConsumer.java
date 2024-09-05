package org.yahaha.mq.consumer;

import com.github.houbb.heaven.util.common.ArgUtil;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.common.constant.ConsumerTypeConst;
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
import org.yahaha.mq.consumer.api.IMQConsumer;
import org.yahaha.mq.consumer.api.IMQConsumerListener;
import org.yahaha.mq.consumer.constant.ConsumerConst;
import org.yahaha.mq.consumer.constant.ConsumerRespCode;
import org.yahaha.mq.consumer.support.broker.ConsumerBrokerConfig;
import org.yahaha.mq.consumer.support.broker.ConsumerBrokerService;
import org.yahaha.mq.consumer.support.broker.IConsumerBrokerService;
import org.yahaha.mq.consumer.support.listener.IMQListenerService;
import org.yahaha.mq.consumer.support.listener.MQListenerService;

@Slf4j
public class MQPushConsumer extends Thread implements IMQConsumer {
    /**
     * 组名称
     */
    protected  String groupName = ConsumerConst.DEFAULT_GROUP_NAME;

    /**
     * 中间人地址
     */
    protected  String brokerAddress  = "127.0.0.1:9999";

    /**
     * 获取响应超时时间
     */
    protected  long respTimeoutMills = 5000;

    /**
     * 检测 broker 可用性
     */
    protected  volatile boolean check = true;

    /**
     * 为剩余的请求等待时间
     */
    protected  long waitMillsForRemainRequest = 60 * 1000;

    /**
     * 调用管理类
     */
    protected  final IInvokeService invokeService = new InvokeService();

    /**
     * 消息监听服务类
     */
    protected final IMQListenerService mqListenerService = new MQListenerService();

    /**
     * 状态管理类
     */
    protected  final IStatusManager statusManager = new StatusManager();

    /**
     * 生产者-中间服务端服务类
     */
    protected  final IConsumerBrokerService consumerBrokerService = new ConsumerBrokerService();

    /**
     * 负载均衡策略
     */
    protected ILoadBalance<RpcChannelFuture> loadBalance = LoadBalances.weightedRoundRobin();

    /**
     * 订阅最大尝试次数
     */
    protected  int subscribeMaxAttempt = 3;

    /**
     * 取消订阅最大尝试次数
     */
    protected  int unSubscribeMaxAttempt = 3;

    public MQPushConsumer subscribeMaxAttempt(int subscribeMaxAttempt) {
        this.subscribeMaxAttempt = subscribeMaxAttempt;
        return this;
    }

    public MQPushConsumer unSubscribeMaxAttempt(int unSubscribeMaxAttempt) {
        this.unSubscribeMaxAttempt = unSubscribeMaxAttempt;
        return this;
    }

    public MQPushConsumer groupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public MQPushConsumer brokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
        return this;
    }

    public MQPushConsumer respTimeoutMills(long respTimeoutMills) {
        this.respTimeoutMills = respTimeoutMills;
        return this;
    }

    public MQPushConsumer check(boolean check) {
        this.check = check;
        return this;
    }

    public MQPushConsumer waitMillsForRemainRequest(long waitMillsForRemainRequest) {
        this.waitMillsForRemainRequest = waitMillsForRemainRequest;
        return this;
    }

    public MQPushConsumer loadBalance(ILoadBalance<RpcChannelFuture> loadBalance) {
        this.loadBalance = loadBalance;
        return this;
    }


    /**
     * 参数校验
     */
    private void paramCheck() {
        ArgUtil.notEmpty(brokerAddress, "brokerAddress");
        ArgUtil.notEmpty(groupName, "groupName");
    }

    @Override
    public void run() {
        // 启动服务端
        log.info("MQ 消费者开始启动服务端 groupName: {}, brokerAddress: {}",
                groupName, brokerAddress);

        //1. 参数校验
        this.paramCheck();

        try {
            //0. 配置信息
            ConsumerBrokerConfig config = ConsumerBrokerConfig.newInstance()
                    .groupName(groupName)
                    .brokerAddress(brokerAddress)
                    .check(true)
                    .respTimeoutMills(respTimeoutMills)
                    .invokeService(invokeService)
                    .statusManager(statusManager)
                    .mqListenerService(mqListenerService)
                    .loadBalance(loadBalance);

            //1. 初始化
            this.consumerBrokerService.initChannelFutureList(config);

            //2. 连接到服务端
            this.consumerBrokerService.registerToBroker();

            //3. 标识为可用
            statusManager.status(true);

            //4. 添加钩子函数
            final DefaultShutdownHook rpcShutdownHook = new DefaultShutdownHook();
            rpcShutdownHook.setStatusManager(statusManager);
            rpcShutdownHook.setInvokeService(invokeService);
            rpcShutdownHook.setWaitMillsForRemainRequest(waitMillsForRemainRequest);
            rpcShutdownHook.setDestroyable(this.consumerBrokerService);
            ShutdownHooks.rpcShutdownHook(rpcShutdownHook);

            // 5. 完成启动后的任务
            this.afterInit();

            log.info("MQ 消费者启动完成");
        } catch (Exception e) {
            log.error("MQ 消费者启动异常", e);
            throw new MQException(ConsumerRespCode.RPC_INIT_FAILED);
        }
    }

    /**
     * 初始化完成以后
     */
    protected void afterInit() {

    }

    /**
     * 获取消费策略类型
     * @return 类型
     */
    protected String getConsumerType() {
        return ConsumerTypeConst.PUSH;
    }


    @Override
    public void subscribe(String topicName, String tagRegex) {
        consumerBrokerService.subscribe(topicName, tagRegex);
    }

    @Override
    public void unSubscribe(String topicName, String tagRegex) {
        consumerBrokerService.unSubscribe(topicName, tagRegex);
    }

    @Override
    public void registerListener(IMQConsumerListener listener) {
        this.mqListenerService.register(listener);
    }


}
