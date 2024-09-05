package org.yahaha.mq.common.loadbalance;

import org.yahaha.mq.common.api.IServer;
import org.yahaha.mq.common.loadbalance.api.AbstractLoadBalance;
import org.yahaha.mq.common.loadbalance.api.ILoadBalanceContext;
import org.yahaha.mq.common.loadbalance.support.ConsistentHashSelector;

import java.util.List;

public class LoadBalanceConsistentHash<T extends IServer> extends AbstractLoadBalance<T> {

    @Override
    protected T doSelect(ILoadBalanceContext<T> context) {
        List<T> servers = context.servers();
        final String hashKey = context.hashKey();
        ConsistentHashSelector<T> selector = new ConsistentHashSelector<T>(16);
        selector.add(servers);
        return selector.get(hashKey);
    }
}
