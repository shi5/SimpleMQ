package org.yahaha.mq.common.support.invoke;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.common.resp.MQCommonRespCode;
import org.yahaha.mq.common.resp.MQException;
import org.yahaha.mq.common.rpc.RpcMessageDto;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class InvokeService implements IInvokeService {


    /**
     * 请求序列号 map
     * （1）这里后期如果要添加超时检测，可以添加对应的超时时间。
     * 可以把这里调整为 map
     *
     * key: seqId 唯一标识一个请求
     * value: 存入该请求最长的有效时间。用于定时删除和超时判断。
     */
    private final ConcurrentHashMap<String, Long> requestMap;

    /**
     * 响应结果
     */
    private final ConcurrentHashMap<String, RpcMessageDto> responseMap;

    public InvokeService() {
        requestMap = new ConcurrentHashMap<>();
        responseMap = new ConcurrentHashMap<>();

        final Runnable timeoutThread = new TimeoutCheckThread(requestMap, responseMap);
        Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(timeoutThread,60, 60, TimeUnit.SECONDS);
    }

    @Override
    public IInvokeService addRequest(String seqId, long timeoutMills) {
        log.debug("[Invoke] start add request for seqId: {}, timeoutMills: {}", seqId,
                timeoutMills);

        final long expireTime = System.currentTimeMillis()+timeoutMills;
        requestMap.putIfAbsent(seqId, expireTime);

        return this;
    }

    @Override
    public IInvokeService addResponse(String seqId, RpcMessageDto rpcResponse) {
        // 1. 判断是否有效
        Long expireTime = this.requestMap.get(seqId);
        // 如果为空，可能是这个结果已经超时了，被定时 job 移除之后，响应结果才过来。直接忽略
        if(ObjectUtil.isNull(expireTime)) {
            return this;
        }

        // TODO: 应该先判断是否存在，如果不存在，直接忽略。，然后判断是否超时。需要考虑并发

        //2. 判断是否超时
        if(System.currentTimeMillis() > expireTime) {
            log.debug("[Invoke] seqId:{} 信息已超时，直接返回超时结果。", seqId);
            rpcResponse = RpcMessageDto.timeout();
        }

        // TODO: 这里放入之前，可以添加判断。
        // 如果 seqId 必须处理请求集合中，才允许放入。或者直接忽略丢弃。
        // 通知所有等待方
        responseMap.putIfAbsent(seqId, rpcResponse);
        log.debug("[Invoke] 获取结果信息，seqId: {}, rpcResponse: {}", seqId, JSON.toJSON(rpcResponse));
        log.debug("[Invoke] seqId:{} 信息已经放入，通知所有等待方", seqId);

        // 移除对应的 requestMap
        requestMap.remove(seqId);
        log.debug("[Invoke] seqId:{} remove from request map", seqId);

        // 同步锁
        synchronized (this) {
            this.notifyAll();
            log.debug("[Invoke] {} notifyAll()", seqId);
        }


        return this;
    }

    @Override
    public RpcMessageDto getResponse(String seqId) {
        try {
            RpcMessageDto rpcResponse = this.responseMap.get(seqId);
            if(ObjectUtil.isNotNull(rpcResponse)) {
                log.debug("[Invoke] seq {} 对应结果已经获取: {}", seqId, rpcResponse);
                return rpcResponse;
            }

            // 进入等待
            if (rpcResponse == null) {
                

                // 同步等待锁
                synchronized (this) {
                    while (this.responseMap.get(seqId) == null) {
                        log.debug("[Invoke] seq {} 对应结果为空，进入等待", seqId);
                        this.wait();
                    }
                }
            }
            log.debug("[Invoke] {} wait has notified!", seqId);

            rpcResponse = this.responseMap.get(seqId);
            log.debug("[Invoke] seq {} 对应结果已经获取: {}", seqId, rpcResponse);

            return rpcResponse;
        } catch (InterruptedException e) {
            log.error("获取响应异常", e);
            throw new MQException(MQCommonRespCode.RPC_GET_RESP_FAILED);
        }
    }

    @Override
    public boolean remainsRequest() {
        for (String seqId : requestMap.keySet()) {
            log.debug("[Invoke] remains request seqId: {}", seqId);
        }

        return !requestMap.isEmpty();
    }

}

