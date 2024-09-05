package org.yahaha.mq.broker;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.api.IBrokerConsumerService;
import org.yahaha.mq.broker.api.IBrokerProducerService;
import org.yahaha.mq.broker.api.IMQBroker;
import org.yahaha.mq.broker.constant.BrokerRespCode;
import org.yahaha.mq.broker.dto.consumer.ConsumerSubscribeBo;
import org.yahaha.mq.broker.handler.MQBrokerHandler;
import org.yahaha.mq.broker.persist.DefaultMQBrokerPersist;
import org.yahaha.mq.broker.persist.IMQBrokerPersist;
import org.yahaha.mq.broker.support.api.LocalBrokerConsumerService;
import org.yahaha.mq.broker.support.api.LocalBrokerProducerService;
import org.yahaha.mq.broker.support.push.BrokerPushService;
import org.yahaha.mq.broker.support.push.IBrokerPushService;
import org.yahaha.mq.broker.support.valid.BrokerRegisterValidService;
import org.yahaha.mq.broker.support.valid.IBrokerRegisterValidService;
import org.yahaha.mq.common.loadbalance.LoadBalances;
import org.yahaha.mq.common.loadbalance.api.ILoadBalance;
import org.yahaha.mq.common.resp.MQException;
import org.yahaha.mq.common.support.invoke.IInvokeService;
import org.yahaha.mq.common.support.invoke.InvokeService;
import org.yahaha.mq.common.util.DelimiterUtil;

@Slf4j
public class MQBroker extends Thread implements IMQBroker {

    /**
     * 端口号
     */
    private int port = 9999;

    /**
     * 调用管理类
     */
    private final IInvokeService invokeService = new InvokeService();

    /**
     * 消费者管理
     */
    private IBrokerConsumerService registerConsumerService = new LocalBrokerConsumerService();

    /**
     * 生产者管理
     */
    private IBrokerProducerService registerProducerService = new LocalBrokerProducerService();

    /**
     * 持久化类
     */
//    private IMQBrokerPersist mqBrokerPersist = new LocalMQBrokerPersist();
    private IMQBrokerPersist mqBrokerPersist = new DefaultMQBrokerPersist();

    /**
     * 推送服务
     */
    private IBrokerPushService brokerPushService = new BrokerPushService();

    /**
     * 获取响应超时时间
     */
    private long respTimeoutMills = 5000;

    /**
     * 负载均衡
     */
    private ILoadBalance<ConsumerSubscribeBo> loadBalance = LoadBalances.weightedRoundRobin();

    /**
     * 推送最大尝试次数
     */
    private int pushMaxAttempt = 3;

    /**
     * 注册验证服务类
     */
    private IBrokerRegisterValidService brokerRegisterValidService = new BrokerRegisterValidService();

    /**
     * 新增消费队列数量
     */
    private int newConsumeQueueNum = 4;

    public MQBroker port(int port) {
        this.port = port;
        return this;
    }

    public MQBroker newConsumeQueueNum(int newConsumeQueueNum) {
        this.newConsumeQueueNum = newConsumeQueueNum;
        return this;
    }

    public MQBroker registerConsumerService(IBrokerConsumerService registerConsumerService) {
        this.registerConsumerService = registerConsumerService;
        return this;
    }

    public MQBroker registerProducerService(IBrokerProducerService registerProducerService) {
        this.registerProducerService = registerProducerService;
        return this;
    }

    public MQBroker mqBrokerPersist(IMQBrokerPersist mqBrokerPersist) {
        this.mqBrokerPersist = mqBrokerPersist;
        return this;
    }

    public MQBroker brokerPushService(IBrokerPushService brokerPushService) {
        this.brokerPushService = brokerPushService;
        return this;
    }

    public MQBroker respTimeoutMills(long respTimeoutMills) {
        this.respTimeoutMills = respTimeoutMills;
        return this;
    }

    public MQBroker loadBalance(ILoadBalance<ConsumerSubscribeBo> loadBalance) {
        this.loadBalance = loadBalance;
        return this;
    }


    private ChannelHandler initChannelHandler() {
        registerConsumerService.loadBalance(this.loadBalance);
        MQBrokerHandler handler = new MQBrokerHandler();
        handler.invokeService(invokeService)
                .respTimeoutMills(respTimeoutMills)
                .registerConsumerService(registerConsumerService)
                .registerProducerService(registerProducerService)
                .mqBrokerPersist(mqBrokerPersist)
                .brokerPushService(brokerPushService)
                .respTimeoutMills(respTimeoutMills)
                .pushMaxAttempt(pushMaxAttempt)
                .newConsumeQueueNum(newConsumeQueueNum);

        return handler;
    }

    @Override
    public void run() {
        // 启动服务端
        log.info("MQ 中间人开始启动服务端 port: {}", port);

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            final ByteBuf delimiterBuf = DelimiterUtil.getByteBuf(DelimiterUtil.DELIMITER);
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(workerGroup, bossGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new DelimiterBasedFrameDecoder(DelimiterUtil.LENGTH, delimiterBuf))
                                    .addLast(initChannelHandler());
                        }
                    })
                    // 这个参数影响的是还没有被accept 取出的连接
                    .option(ChannelOption.SO_BACKLOG, 128)
                    // 这个参数只是过一段时间内客户端没有响应，服务端会发送一个 ack 包，以判断客户端是否还活着。
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            // 绑定端口，开始接收进来的链接
            ChannelFuture channelFuture = serverBootstrap.bind(port).syncUninterruptibly();
            log.info("MQ 中间人启动完成，监听【" + port + "】端口");

            channelFuture.channel().closeFuture().syncUninterruptibly();
            log.info("MQ 中间人关闭完成");
        } catch (Exception e) {
            log.error("MQ 中间人启动异常", e);
            throw new MQException(BrokerRespCode.RPC_INIT_FAILED);
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

}
