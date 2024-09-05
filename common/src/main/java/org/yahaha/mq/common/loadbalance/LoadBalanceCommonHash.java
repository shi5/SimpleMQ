package org.yahaha.mq.common.loadbalance;

import org.yahaha.mq.common.loadbalance.api.AbstractLoadBalance;
import org.yahaha.mq.common.loadbalance.api.ILoadBalanceContext;
import org.yahaha.mq.common.api.IServer;

import java.util.List;

/**
 * 普通哈希
 * 使用JDK自带的hashcode方法
 *
 * @param <T> 服务类型
 */
public class LoadBalanceCommonHash<T extends IServer> extends AbstractLoadBalance<T> {
    @Override
    protected T doSelect(ILoadBalanceContext<T> context) {
        List<T> servers = context.servers();

        final String hashKey = context.hashKey();
        int code = hashKey.hashCode();
        int hashCode = Math.abs(code);
        int index = hashCode % servers.size();
        return servers.get(index);
    }
}
