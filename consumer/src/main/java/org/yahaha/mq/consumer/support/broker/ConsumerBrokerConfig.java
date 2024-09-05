package org.yahaha.mq.consumer.support.broker;

import org.yahaha.mq.common.loadbalance.api.ILoadBalance;
import org.yahaha.mq.common.rpc.RpcChannelFuture;
import org.yahaha.mq.common.support.invoke.IInvokeService;
import org.yahaha.mq.common.support.status.IStatusManager;
import org.yahaha.mq.consumer.support.listener.IMQListenerService;

public class ConsumerBrokerConfig {
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
     * 负载均衡
     */
    private ILoadBalance<RpcChannelFuture> loadBalance;

    /**
     * 订阅最大尝试次数
     */
    private int subscribeMaxAttempt = 3;

    /**
     * 取消订阅最大尝试次数
     */
    private int unSubscribeMaxAttempt = 3;

    /**
     * 消费状态更新最大尝试次数
     */
    private int consumerStatusMaxAttempt = 3;
    
    /**
     * 消费最大尝试次数
     */
    private int consumeMaxAttempt = 3;

    public static ConsumerBrokerConfig newInstance() {
        return new ConsumerBrokerConfig();
    }
    
    public int consumeMaxAttempt() {
        return consumeMaxAttempt;
    }
    
    public ConsumerBrokerConfig consumeMaxAttempt(int consumeMaxAttempt) {
        this.consumeMaxAttempt = consumeMaxAttempt;
        return this;
    }

    public String groupName() {
        return groupName;
    }

    public ConsumerBrokerConfig groupName(String groupName) {
        this.groupName = groupName;
        return this;
    }

    public String brokerAddress() {
        return brokerAddress;
    }

    public ConsumerBrokerConfig brokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
        return this;
    }

    public IInvokeService invokeService() {
        return invokeService;
    }

    public ConsumerBrokerConfig invokeService(IInvokeService invokeService) {
        this.invokeService = invokeService;
        return this;
    }

    public long respTimeoutMills() {
        return respTimeoutMills;
    }

    public ConsumerBrokerConfig respTimeoutMills(long respTimeoutMills) {
        this.respTimeoutMills = respTimeoutMills;
        return this;
    }

    public boolean check() {
        return check;
    }

    public ConsumerBrokerConfig check(boolean check) {
        this.check = check;
        return this;
    }

    public IStatusManager statusManager() {
        return statusManager;
    }

    public ConsumerBrokerConfig statusManager(IStatusManager statusManager) {
        this.statusManager = statusManager;
        return this;
    }

    public IMQListenerService mqListenerService() {
        return mqListenerService;
    }

    public ConsumerBrokerConfig mqListenerService(IMQListenerService mqListenerService) {
        this.mqListenerService = mqListenerService;
        return this;
    }

    public ILoadBalance<RpcChannelFuture> loadBalance() {
        return loadBalance;
    }

    public ConsumerBrokerConfig loadBalance(ILoadBalance<RpcChannelFuture> loadBalance) {
        this.loadBalance = loadBalance;
        return this;
    }

    public int consumerStatusMaxAttempt() {
        return consumerStatusMaxAttempt;
    }

    public ConsumerBrokerConfig consumerStatusMaxAttempt(int consumerStatusMaxAttempt) {
        this.consumerStatusMaxAttempt = consumerStatusMaxAttempt;
        return this;
    }

    public int subscribeMaxAttempt() {
        return subscribeMaxAttempt;
    }

    public ConsumerBrokerConfig subscribeMaxAttempt(int subscribeMaxAttempt) {
        this.subscribeMaxAttempt = subscribeMaxAttempt;
        return this;
    }

    public int unSubscribeMaxAttempt() {
        return unSubscribeMaxAttempt;
    }

    public ConsumerBrokerConfig unSubscribeMaxAttempt(int unSubscribeMaxAttempt) {
        this.unSubscribeMaxAttempt = unSubscribeMaxAttempt;
        return this;
    }
}
