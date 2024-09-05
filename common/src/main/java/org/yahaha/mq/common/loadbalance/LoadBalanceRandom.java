package org.yahaha.mq.common.loadbalance;

import org.yahaha.mq.common.loadbalance.api.AbstractLoadBalance;
import org.yahaha.mq.common.loadbalance.api.ILoadBalanceContext;
import org.yahaha.mq.common.api.IServer;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class LoadBalanceRandom<T extends IServer> extends AbstractLoadBalance<T> {
    @Override
    protected T doSelect(ILoadBalanceContext<T> context) {
        List<T> servers = context.servers();
        int size = servers.size();
        if (size == 0) {
            return null;
        }
        Random random = ThreadLocalRandom.current();
        return servers.get(random.nextInt(size));
    }
}
