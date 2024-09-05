package org.yahaha.mq.common;

import org.yahaha.mq.common.api.IServer;
import org.yahaha.mq.common.loadbalance.LoadBalances;
import org.yahaha.mq.common.loadbalance.api.ILoadBalance;
import org.yahaha.mq.common.util.LoadBalanceUtil;

import java.util.Arrays;
import java.util.List;

public class TestLoadBalance<T> {

    public static void main(String[] args) {
        ILoadBalance<Server> loadBalance = LoadBalances.roundRobin();
        List<Server> servers1 = Arrays.asList(new Server(0, "张三"), new Server(1,"张三"));
        List<Server> servers2 = Arrays.asList(new Server(0, "李四"), new Server(1,"李四"), new Server(2,"李四"));

        for (int i = 0; i < 10; i++) {
            System.out.println(LoadBalanceUtil.loadBalance(loadBalance, servers1));
            System.out.println(LoadBalanceUtil.loadBalance(loadBalance, servers2));
        }

    }

    static class Server implements IServer {
        public int id;
        private String name;
        public Server(int id, String name) {
            this.id = id;
            this.name = name;
        }

        public String toString() {
            return "Server{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    '}';
        }

        @Override
        public String url() {
            return null;
        }

        @Override
        public int weight() {
            return 0;
        }
    }
}
