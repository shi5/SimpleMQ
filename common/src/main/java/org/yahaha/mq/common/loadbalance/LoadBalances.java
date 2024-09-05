package org.yahaha.mq.common.loadbalance;


import org.yahaha.mq.common.api.IServer;
import org.yahaha.mq.common.loadbalance.api.ILoadBalance;

public final class LoadBalances {
    private LoadBalances(){}

    /**
     * 随机
     */
    public static <T extends IServer> ILoadBalance<T> random() {
        return new LoadBalanceRandom<>();
    }

    /**
     * 轮训
     */
    public static <T extends IServer> ILoadBalance<T> roundRobin() {
        return new LoadBalanceRoundRobin<>();
    }

    /**
     * 权重轮训
     */
    public static <T extends IServer> ILoadBalance<T> weightedRoundRobin() {
        return new LoadBalanceWeightedRoundRobin<>();
    }

    /**
     * 普通 Hash
     */
    public static <T extends IServer> ILoadBalance<T> commonHash() {
        return new LoadBalanceCommonHash<>();
    }

    /**
     * 一致性 Hash
     */
    public static <T extends IServer> ILoadBalance<T> consistentHash() {
        return new LoadBalanceConsistentHash<>();
    }
}
