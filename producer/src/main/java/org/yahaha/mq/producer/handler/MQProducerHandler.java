package org.yahaha.mq.producer.handler;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.common.rpc.RpcMessageDto;
import org.yahaha.mq.common.support.invoke.IInvokeService;
import org.yahaha.mq.common.util.ChannelUtil;

@Slf4j
public class MQProducerHandler extends SimpleChannelInboundHandler {

    /**
     * 调用管理类
     */
    private IInvokeService invokeService;

    public void setInvokeService(IInvokeService invokeService) {
        this.invokeService = invokeService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf)msg;
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);

        String text = new String(bytes);
        log.debug("[Client] channelId {} 接收到消息 {}", ChannelUtil.getChannelId(ctx), text);

        RpcMessageDto rpcMessageDto = null;
        try {
            rpcMessageDto = JSON.parseObject(bytes, RpcMessageDto.class);
        } catch (Exception exception) {
            log.error("RpcMessageDto json 格式转换异常 {}", JSON.parse(bytes));
            return;
        }

        if(rpcMessageDto.isRequest()) {
            // 请求类
            final String methodType = rpcMessageDto.getMethodType();
            final String json = rpcMessageDto.getJson();
        } else {
            // 丢弃掉 traceId 为空的信息
            if(StrUtil.isBlank(rpcMessageDto.getTraceId())) {
                log.debug("[Client] response traceId 为空，直接丢弃", JSON.toJSON(rpcMessageDto));
                return;
            }

            invokeService.addResponse(rpcMessageDto.getTraceId(), rpcMessageDto);
            log.debug("[Client] response is :{}", JSON.toJSON(rpcMessageDto));
        }
    }
}
