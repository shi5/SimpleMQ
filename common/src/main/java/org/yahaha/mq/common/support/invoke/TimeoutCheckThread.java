package org.yahaha.mq.common.support.invoke;

import cn.hutool.core.util.ObjectUtil;
import org.yahaha.mq.common.rpc.RpcMessageDto;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TimeoutCheckThread implements Runnable {

    /**
     * 请求信息
     */
    private final ConcurrentHashMap<String, Long> requestMap;

    /**
     * 请求信息
     */
    private final ConcurrentHashMap<String, RpcMessageDto> responseMap;

    /**
     * 新建
     * @param requestMap  请求 Map
     * @param responseMap 结果 map
     */
    public TimeoutCheckThread(ConcurrentHashMap<String, Long> requestMap,
                              ConcurrentHashMap<String, RpcMessageDto> responseMap) {
        if (ObjectUtil.isNull(requestMap)) {
            throw new IllegalArgumentException("requestMap 不能为空");
        }
        this.requestMap = requestMap;
        this.responseMap = responseMap;
    }

    @Override
    public void run() {
        for(Map.Entry<String, Long> entry : requestMap.entrySet()) {
            long expireTime = entry.getValue();
            long currentTime = System.currentTimeMillis();

            if(currentTime > expireTime) {
                final String key = entry.getKey();
                // 结果设置为超时，从请求 map 中移除
                responseMap.putIfAbsent(key, RpcMessageDto.timeout());
                requestMap.remove(key);
            }
        }
    }

}
