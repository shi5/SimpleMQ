package org.yahaha.mq.common.loadbalance.api;

import org.yahaha.mq.common.api.IServer;

import java.util.List;

public class LoadBalanceContext<T extends IServer> implements ILoadBalanceContext<T>{
    private String hashKey;
    private List<T> servers;

    public static <T extends IServer> LoadBalanceContext<T> newInstance() {
        return new LoadBalanceContext<>();
    }

    public LoadBalanceContext<T> hashKey(String hashKey) {
        this.hashKey = hashKey;
        return this;
    }

    @Override
    public String hashKey() {
        return hashKey;
    }

    public LoadBalanceContext<T> servers(List<T> servers) {
        this.servers = servers;
        return this;
    }

    @Override
    public List<T> servers() {
        return servers;
    }

    @Override
    public String toString() {
        return "LoadBalanceContext{" +
                "hashKey='" + hashKey + '\'' +
                ", servers=" + servers +
                '}';
    }
}
