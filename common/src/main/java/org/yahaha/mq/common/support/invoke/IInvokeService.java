package org.yahaha.mq.common.support.invoke;

import org.yahaha.mq.common.rpc.RpcMessageDto;

public interface IInvokeService {
    /**
     * 添加请求信息
     * @param seqId 序列号
     * @param timeoutMills 超时时间
     * @return this
     */
    IInvokeService addRequest(final String seqId,
                              final long timeoutMills);

    /**
     * 放入结果
     * @param seqId 唯一标识
     * @param rpcResponse 响应结果
     * @return this
     */
    IInvokeService addResponse(final String seqId, final RpcMessageDto rpcResponse);

    /**
     * 获取标志信息对应的结果
     * @param seqId 序列号
     * @return 结果
     */
    RpcMessageDto getResponse(final String seqId);

    /**
     * 是否依然包含请求待处理
     * @return 是否
     */
    boolean remainsRequest();
}
