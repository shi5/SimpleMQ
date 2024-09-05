package org.yahaha.mq.common.loadbalance.api;

import org.yahaha.mq.common.api.IServer;

import java.util.List;

public interface ILoadBalanceContext<T extends IServer> {
    String hashKey();

    List<T> servers();
}