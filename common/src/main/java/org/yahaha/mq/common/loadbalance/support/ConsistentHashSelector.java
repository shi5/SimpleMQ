package org.yahaha.mq.common.loadbalance.support;

import org.yahaha.mq.common.api.IServer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class ConsistentHashSelector<T  extends IServer> {
    private final int virtualNum;
    private final TreeMap<Integer, T> nodeMap = new TreeMap<>();

    public ConsistentHashSelector(int virtualNum) {
        this.virtualNum = virtualNum;
    }

    /**
     * 沿环的顺时针找到虚拟节点
     * @param key key
     * @return 结果
     */
    public T get(String key) {
        final int hashCode = key.hashCode();
        Integer target = hashCode;

        // 不包含时候的处理
        if (!nodeMap.containsKey(hashCode)) {
            target = nodeMap.ceilingKey(hashCode);
            if (target == null && !nodeMap.isEmpty()) {
                target = nodeMap.firstKey();
            }
        }
        return nodeMap.get(target);
    }

    // 添加服务节点
    public ConsistentHashSelector<T> add(T node) {
        // 初始化虚拟节点
        for (int i = 0; i < virtualNum; i++) {
            int nodeKey = (node.toString() + "-" + i).hashCode();
            nodeMap.put(nodeKey, node);
        }

        return this;
    }

    public ConsistentHashSelector<T> add(Collection<T> nodes) {
        // 初始化虚拟节点
        for (T node : nodes) {
            for (int i = 0; i < virtualNum; i++) {
                int nodeKey = (node.toString() + "-" + i).hashCode();
                nodeMap.put(nodeKey, node);
            }
        }

        return this;
    }

    // 移除服务节点
    public ConsistentHashSelector<T> remove(T node) {
        // 移除虚拟节点
        // 其实这里有一个问题，如果存在 hash 冲突，直接移除会不会不够严谨？
        for (int i = 0; i < virtualNum; i++) {
            int nodeKey = (node.toString() + "-" + i).hashCode();
            nodeMap.remove(nodeKey);
        }

        return this;
    }

    public Map<Integer, T> nodeMap() {
        return Collections.unmodifiableMap(this.nodeMap);
    }
}
