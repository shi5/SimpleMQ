package org.yahaha.mq.common.loadbalance;

import org.yahaha.mq.common.loadbalance.api.AbstractLoadBalance;
import org.yahaha.mq.common.loadbalance.api.ILoadBalanceContext;
import org.yahaha.mq.common.api.IServer;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LoadBalanceRoundRobin<T extends IServer> extends AbstractLoadBalance<T> {
    private final AtomicLong indexHolder = new AtomicLong();

    @Override
    protected T doSelect(ILoadBalanceContext<T> context) {
        List<T> servers = context.servers();
        int size = servers.size();
        if (size == 0) {
            return null;
        }
        long index = indexHolder.getAndIncrement();
        return servers.get((int) (index % size));
    }
}
