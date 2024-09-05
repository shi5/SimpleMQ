package org.yahaha.mq.common.util;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.common.rpc.RpcAddress;
import org.yahaha.mq.common.rpc.RpcChannelFuture;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ChannelFutureUtil {
    public static List<RpcChannelFuture> initChannelFutureList(final String brokerAddress,
                                                               final ChannelHandler channelHandler) {
        List<RpcAddress> addressList = InnerAddressUtil.initAddressList(brokerAddress);

        List<RpcChannelFuture> list = new ArrayList<>();
        for(RpcAddress rpcAddress : addressList) {
            final String address = rpcAddress.getAddress();
            final int port = rpcAddress.getPort();

            EventLoopGroup workerGroup = new NioEventLoopGroup();
            Bootstrap bootstrap = new Bootstrap();
            ChannelFuture channelFuture = bootstrap.group(workerGroup)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .handler(new ChannelInitializer<Channel>(){
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new LoggingHandler(LogLevel.INFO))
                                    .addLast(channelHandler);
                        }
                    })
                    .connect(address, port)
                    .syncUninterruptibly();

            log.info("启动客户端完成，监听 address: {}, port：{}", address, port);

            RpcChannelFuture rpcChannelFuture = new RpcChannelFuture();
            rpcChannelFuture.setChannelFuture(channelFuture);
            rpcChannelFuture.setAddress(address);
            rpcChannelFuture.setPort(port);
            rpcChannelFuture.setWeight(rpcAddress.getWeight());
            list.add(rpcChannelFuture);
        }

        return list;
    }

}
