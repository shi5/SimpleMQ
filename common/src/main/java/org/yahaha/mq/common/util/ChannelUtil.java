package org.yahaha.mq.common.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

public class ChannelUtil {
    private ChannelUtil(){}

    /**
     * 获取 channel 标识
     * @param channel 管道
     * @return 结果
     *      */
    public static String getChannelId(Channel channel) {
        return channel.id().asLongText();
    }

    /**
     * 获取 channel 标识
     * @param ctx 管道
     * @return 结果
     *      */
    public static String getChannelId(ChannelHandlerContext ctx) {
        return getChannelId(ctx.channel());
    }
}
