package org.yahaha.mq.common.loadbalance.api;

import org.yahaha.mq.common.api.IServer;

public interface ILoadBalance<T extends IServer> {
    /**
     * 选择下一个节点
     *
     * 返回下标
     * @param cxt 上下文
     * @return 结果
     */
    T select(final ILoadBalanceContext<T> cxt);
}
