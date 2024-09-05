package org.yahaha.mq.broker.support.api;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.api.IBrokerProducerService;
import org.yahaha.mq.broker.dto.BrokerServiceEntryChannel;
import org.yahaha.mq.broker.dto.ServiceEntry;
import org.yahaha.mq.broker.util.InnerChannelUtil;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.resp.MQCommonRespCode;
import org.yahaha.mq.common.util.ChannelUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class LocalBrokerProducerService implements IBrokerProducerService {

    private final Map<String, BrokerServiceEntryChannel> registerMap = new ConcurrentHashMap<>();

    @Override
    public MQCommonResp register(ServiceEntry serviceEntry, Channel channel) {
        final String channelId = ChannelUtil.getChannelId(channel);
        BrokerServiceEntryChannel entryChannel = InnerChannelUtil.buildEntryChannel(serviceEntry, channel);
        registerMap.put(channelId, entryChannel);


        MQCommonResp resp = new MQCommonResp();
        resp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return resp;
    }

    @Override
    public MQCommonResp unRegister(ServiceEntry serviceEntry, Channel channel) {
        final String channelId = ChannelUtil.getChannelId(channel);
        registerMap.remove(channelId);

        MQCommonResp resp = new MQCommonResp();
        resp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return resp;
    }

    @Override
    public ServiceEntry getServiceEntry(String channelId) {
        return registerMap.get(channelId);
    }

}
