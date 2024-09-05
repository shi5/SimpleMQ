package org.yahaha.mq.common.loadbalance.api;

import org.yahaha.mq.common.api.IServer;

import java.util.List;

// TODO: 目前每次负载均衡都需要重新计算，可以考虑缓存
public abstract class AbstractLoadBalance<T extends IServer> implements ILoadBalance<T>{
    public AbstractLoadBalance() {
    }

    @Override
    public T select(ILoadBalanceContext<T> context) {
        List<T> servers = context.servers();
        return servers.size() <= 1 ? servers.get(0) : this.doSelect(context);
    }

    protected abstract T doSelect(ILoadBalanceContext<T> var1);
}
