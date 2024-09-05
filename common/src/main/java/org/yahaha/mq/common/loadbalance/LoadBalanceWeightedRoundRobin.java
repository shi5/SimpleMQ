package org.yahaha.mq.common.loadbalance;

import org.yahaha.mq.common.api.IServer;
import org.yahaha.mq.common.loadbalance.api.AbstractLoadBalance;
import org.yahaha.mq.common.loadbalance.api.ILoadBalanceContext;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 加权轮询
 */
public class LoadBalanceWeightedRoundRobin<T extends IServer> extends AbstractLoadBalance<T> {
    /**
     * 过期时间
     */
    private static int RECYCLE_PERIOD = 60000;

    protected static class WeightedRoundRobin {
        private int weight;
        private AtomicLong currentWeight;
        private long lastUpdate;

        public long getLastUpdate() {
            return lastUpdate;
        }

        public void setLastUpdate(long lastUpdate) {
            this.lastUpdate = lastUpdate;
        }

        public void setWeight(int weight) {
            this.weight = weight;
            currentWeight.set(0);
        }
        public int getWeight() {
            return weight;
        }
        public long increaseCurrent() {
            return currentWeight.addAndGet(weight);
        }
        public void sel(int total) {
            currentWeight.addAndGet(-1 * total);
        }
    }

    private ConcurrentHashMap<String, WeightedRoundRobin> weightMap = new ConcurrentHashMap<>();

    private AtomicBoolean updateLock = new AtomicBoolean();

    @Override
    protected T doSelect(ILoadBalanceContext<T> context) {
        List<T> servers = context.servers();
        int totalWeight = 0;
        long maxCurrent = Long.MIN_VALUE;
        long now = System.currentTimeMillis();
        T selectedServer = null;
        WeightedRoundRobin selectedWRR = null;
        for (T server : servers) {
            String key = server.url();
            WeightedRoundRobin wrr = weightMap.get(key);
            int weight = server.weight();
            if (wrr == null) {
                wrr = new WeightedRoundRobin();
                wrr.setWeight(weight);
                weightMap.putIfAbsent(key, wrr);
                wrr = weightMap.get(key);
            }
            if (weight != wrr.getWeight()) {
                wrr.setWeight(server.weight());
            }
            wrr.setLastUpdate(now);
            long current = wrr.increaseCurrent();
            if (current > maxCurrent) {
                maxCurrent = current;
                selectedServer = server;
                selectedWRR = wrr;
            }
            totalWeight += wrr.getWeight();
        }

        if (!updateLock.get() && servers.size() != weightMap.size()) {
            if (updateLock.compareAndSet(false, true)) {
                try {
                    ConcurrentHashMap<String, WeightedRoundRobin> newWeightMap = new ConcurrentHashMap<>();
                    newWeightMap.putAll(weightMap);
                    Iterator<Entry<String, WeightedRoundRobin>> it = newWeightMap.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, WeightedRoundRobin> item = it.next();
                        if (now - item.getValue().getLastUpdate() > RECYCLE_PERIOD) {
                            it.remove();
                        }
                    }
                    weightMap = newWeightMap;
                } finally {
                    updateLock.set(false);
                }
            }
        }

        if (selectedServer != null) {
            selectedWRR.sel(totalWeight);
            return selectedServer;
        }

        return servers.get(0);
    }
}
