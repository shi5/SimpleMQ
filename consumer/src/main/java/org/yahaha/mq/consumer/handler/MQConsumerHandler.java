package org.yahaha.mq.consumer.handler;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.common.constant.MethodType;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.dto.resp.MQConsumerResultResp;
import org.yahaha.mq.common.resp.ConsumerStatus;
import org.yahaha.mq.common.resp.MQCommonRespCode;
import org.yahaha.mq.common.rpc.RpcMessageDto;
import org.yahaha.mq.common.support.invoke.IInvokeService;
import org.yahaha.mq.common.util.ChannelUtil;
import org.yahaha.mq.common.util.DelimiterUtil;
import org.yahaha.mq.consumer.api.IMQConsumerListenerContext;
import org.yahaha.mq.consumer.support.listener.IMQListenerService;
import org.yahaha.mq.consumer.support.listener.MQConsumerListenerContext;

@Slf4j
public class MQConsumerHandler extends SimpleChannelInboundHandler {

    /**
     * 调用管理类
     */
    private final IInvokeService invokeService;

    /**
     * 消息监听服务类
     */
    private final IMQListenerService mqListenerService;

    public MQConsumerHandler(IInvokeService invokeService, IMQListenerService mqListenerService) {
        this.invokeService = invokeService;
        this.mqListenerService = mqListenerService;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;
        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);

        RpcMessageDto rpcMessageDto = null;
        try {
            rpcMessageDto = JSON.parseObject(bytes, RpcMessageDto.class);
        } catch (Exception exception) {
            log.error("RpcMessageDto json 格式转换异常 {}", new String(bytes));
            return;
        }

        if (rpcMessageDto.isRequest()) {
            MQCommonResp commonResp = this.dispatch(rpcMessageDto, ctx);

            if(commonResp == null) {
                log.debug("当前消息为 null，忽略处理。");
                return;
            }

            writeResponse(rpcMessageDto, commonResp, ctx);
        } else {
            final String traceId = rpcMessageDto.getTraceId();

            // 丢弃掉 traceId 为空的信息
            if(StrUtil.isBlank(traceId)) {
                log.debug("[Server Response] response traceId 为空，直接丢弃", JSON.toJSON(rpcMessageDto));
                return;
            }

            // 添加消息
            invokeService.addResponse(traceId, rpcMessageDto);
        }
    }

    /**
     * 消息的分发
     *
     * @param rpcMessageDto 入参
     * @param ctx 上下文
     * @return 结果
     */
    private MQCommonResp dispatch(RpcMessageDto rpcMessageDto, ChannelHandlerContext ctx) {
        final String methodType = rpcMessageDto.getMethodType();
        final String json = rpcMessageDto.getJson();
        String channelId = ChannelUtil.getChannelId(ctx);
        log.debug("channelId: {} 接收到 method: {} 内容：{}", channelId,
                methodType, json);

        // 消息发送
        if(MethodType.B_MESSAGE_PUSH.equals(methodType)) {
            // 日志输出
            log.info("收到服务端消息: {}", json);
            return this.consumer(json);
        }
        throw new UnsupportedOperationException("暂不支持的方法类型");
    }

    /**
     * 消息消费
     * @param json 原始请求
     * @return 结果
     *      */
    private MQCommonResp consumer(final String json) {
        try {
            MQMessage mqMessage = JSON.parseObject(json, MQMessage.class);
            IMQConsumerListenerContext context = new MQConsumerListenerContext();
            ConsumerStatus consumerStatus = this.mqListenerService.consumer(mqMessage, context);

            MQConsumerResultResp resp = new MQConsumerResultResp();
            resp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
            resp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
            resp.setConsumerStatus(consumerStatus.getCode());
            return resp;
        } catch (Exception exception) {
            log.error("消息消费异常", exception);
            MQConsumerResultResp resp = new MQConsumerResultResp();
            resp.setRespCode(MQCommonRespCode.FAIL.getCode());
            resp.setRespMessage(MQCommonRespCode.FAIL.getMsg());
            return resp;
        }
    }

    /**
     * 结果写回
     *
     * @param req  请求
     * @param resp 响应
     * @param ctx  上下文
     */
    private void writeResponse(RpcMessageDto req, Object resp, ChannelHandlerContext ctx) {
        final String id = ctx.channel().id().asLongText();
        RpcMessageDto rpcMessageDto = new RpcMessageDto();
        // 响应类消息
        rpcMessageDto.setRequest(false);
        rpcMessageDto.setTraceId(req.getTraceId());
        rpcMessageDto.setMethodType(req.getMethodType());
        rpcMessageDto.setRequestTime(System.currentTimeMillis());
        String json = JSON.toJSONString(resp);
        rpcMessageDto.setJson(json);
        // 回写到 client 端
        ByteBuf byteBuf = DelimiterUtil.getMessageDelimiterBuffer(rpcMessageDto);
        ctx.writeAndFlush(byteBuf);
        log.debug("[Server] channel {} response {}", id, JSON.toJSON(rpcMessageDto));
    }
}
