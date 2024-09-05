package org.yahaha.mq.common.util;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import org.yahaha.mq.common.api.IServer;
import org.yahaha.mq.common.loadbalance.api.ILoadBalance;
import org.yahaha.mq.common.loadbalance.api.LoadBalanceContext;

import java.util.List;
import java.util.Objects;

public class LoadBalanceUtil {
    /**
     * 随机
     * @param list 列表
     * @param key 分片键
     * @param <T> 泛型
     * @return 结果
     */
    public static <T> T random(final List<T> list, String key) {
        if(CollectionUtil.isEmpty(list)) {
            return null;
        }

        if(StrUtil.isEmpty(key)) {
            int size = list.size();
            int index = RandomUtil.randomInt(size);
            return list.get(index);
        }

        // 获取 code
        int hashCode = Objects.hash(key);
        int index = hashCode % list.size();
        return list.get(index);
    }

    /**
     * 负载均衡
     *
     * @param list 列表
     * @param key 分片键
     * @return 结果
     */
    public static <T extends IServer> T loadBalance(final ILoadBalance<T> loadBalance,
                                                    final List<T> list, String key) {
        if(CollectionUtil.isEmpty(list)) {
            return null;
        }

        if(StrUtil.isEmpty(key)) {
            LoadBalanceContext<T> loadBalanceContext = LoadBalanceContext.<T>newInstance()
                    .servers(list);
            return loadBalance.select(loadBalanceContext);
        }

        // 获取 code
        int hashCode = Objects.hash(key);
        int index = hashCode % list.size();
        return list.get(index);
    }

    /**
     * 负载均衡
     *
     * @param list 列表
     * @return 结果
     */
    public static <T extends IServer> T loadBalance(final ILoadBalance<T> loadBalance,
                                                    final List<T> list) {
        return loadBalance(loadBalance, list, null);
    }
}
