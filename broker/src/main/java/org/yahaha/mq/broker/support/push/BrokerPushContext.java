package org.yahaha.mq.broker.support.push;

import org.yahaha.mq.broker.dto.ChannelGroupNameDto;
import org.yahaha.mq.broker.persist.IMQBrokerPersist;
import org.yahaha.mq.broker.dto.persist.MQMessagePersistPut;
import org.yahaha.mq.common.support.invoke.IInvokeService;

import java.util.List;
import java.util.Map;


public class BrokerPushContext {

    private IMQBrokerPersist mqBrokerPersist;

    private MQMessagePersistPut mqMessagePersistPut;

    private List<ChannelGroupNameDto> channelList;

    private IInvokeService invokeService;

    /**
     * 获取响应超时时间
     */
    private long respTimeoutMills;

    /**
     * 推送最大尝试次数
     */
    private int pushMaxAttempt;

    /**
     * channel 标识和 groupName map
     */
    private Map<String, String> channelGroupMap;


    public static BrokerPushContext newInstance() {
        return new BrokerPushContext();
    }

    public IMQBrokerPersist mqBrokerPersist() {
        return mqBrokerPersist;
    }

    public BrokerPushContext mqBrokerPersist(IMQBrokerPersist mqBrokerPersist) {
        this.mqBrokerPersist = mqBrokerPersist;
        return this;
    }

    public MQMessagePersistPut mqMessagePersistPut() {
        return mqMessagePersistPut;
    }

    public BrokerPushContext mqMessagePersistPut(MQMessagePersistPut mqMessagePersistPut) {
        this.mqMessagePersistPut = mqMessagePersistPut;
        return this;
    }

    public List<ChannelGroupNameDto> channelList() {
        return channelList;
    }

    public BrokerPushContext channelList(List<ChannelGroupNameDto> channelList) {
        this.channelList = channelList;
        return this;
    }

    public IInvokeService invokeService() {
        return invokeService;
    }

    public BrokerPushContext invokeService(IInvokeService invokeService) {
        this.invokeService = invokeService;
        return this;
    }

    public long respTimeoutMills() {
        return respTimeoutMills;
    }

    public BrokerPushContext respTimeoutMills(long respTimeoutMills) {
        this.respTimeoutMills = respTimeoutMills;
        return this;
    }

    public int pushMaxAttempt() {
        return pushMaxAttempt;
    }

    public BrokerPushContext pushMaxAttempt(int pushMaxAttempt) {
        this.pushMaxAttempt = pushMaxAttempt;
        return this;
    }

    public Map<String, String> channelGroupMap() {
        return channelGroupMap;
    }

    public BrokerPushContext channelGroupMap(Map<String, String> channelGroupMap) {
        this.channelGroupMap = channelGroupMap;
        return this;
    }
}