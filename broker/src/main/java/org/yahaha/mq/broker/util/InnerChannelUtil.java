package org.yahaha.mq.broker.util;

import io.netty.channel.Channel;
import org.yahaha.mq.broker.dto.BrokerServiceEntryChannel;
import org.yahaha.mq.broker.dto.ServiceEntry;
import org.yahaha.mq.common.rpc.RpcChannelFuture;

public class InnerChannelUtil {
    private InnerChannelUtil(){}

    /**
     * 构建基本服务地址
     * @param rpcChannelFuture 信息
     * @return 结果
     */
    public static ServiceEntry buildServiceEntry(RpcChannelFuture rpcChannelFuture) {
        ServiceEntry serviceEntry = new ServiceEntry();

        serviceEntry.setAddress(rpcChannelFuture.getAddress());
        serviceEntry.setPort(rpcChannelFuture.getPort());
        serviceEntry.setWeight(rpcChannelFuture.getWeight());
        return serviceEntry;
    }

    public static BrokerServiceEntryChannel buildEntryChannel(ServiceEntry serviceEntry,
                                                              Channel channel) {
        BrokerServiceEntryChannel result = new BrokerServiceEntryChannel();
        result.setChannel(channel);
        result.setGroupName(serviceEntry.getGroupName());
        result.setAddress(serviceEntry.getAddress());
        result.setPort(serviceEntry.getPort());
        result.setWeight(serviceEntry.getWeight());
        return result;
    }
}
