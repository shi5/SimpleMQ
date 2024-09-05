package org.yahaha.mq.broker.support.api;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.ReUtil;
import com.alibaba.fastjson.JSON;
import com.github.houbb.heaven.util.util.MapUtil;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.api.IBrokerConsumerService;
import org.yahaha.mq.broker.dto.BrokerServiceEntryChannel;
import org.yahaha.mq.broker.dto.ChannelGroupNameDto;
import org.yahaha.mq.broker.dto.ServiceEntry;
import org.yahaha.mq.broker.dto.consumer.ConsumerSubscribeBo;
import org.yahaha.mq.broker.dto.consumer.ConsumerSubscribeReq;
import org.yahaha.mq.broker.dto.consumer.ConsumerUnSubscribeReq;
import org.yahaha.mq.broker.util.InnerChannelUtil;
import org.yahaha.mq.common.dto.req.MQHeartBeatReq;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.loadbalance.api.ILoadBalance;
import org.yahaha.mq.common.resp.MQCommonRespCode;
import org.yahaha.mq.common.util.ChannelUtil;
import org.yahaha.mq.common.util.LoadBalanceUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

@Slf4j
public class LocalBrokerConsumerService implements IBrokerConsumerService {

    /**
     * 注册集合
     * key: channelId
     * value: 对应的服务信息
     */
    private final Map<String, BrokerServiceEntryChannel> registerMap = new ConcurrentHashMap<>();

    /**
     * 订阅集合-推送策略
     * key: topicName
     * value: 对应的订阅列表
     */
    private final Map<String, Set<ConsumerSubscribeBo>> pushSubscribeMap = new ConcurrentHashMap<>();

    /**
     * 心跳 map
     */
    private final Map<String, BrokerServiceEntryChannel> heartbeatMap = new ConcurrentHashMap<>();

    /**
     * 心跳定时任务
     */
    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    /**
     * 负载均衡策略
     */
    private ILoadBalance<ConsumerSubscribeBo> loadBalance;

    @Override
    public void loadBalance(ILoadBalance<ConsumerSubscribeBo> loadBalance) {
        this.loadBalance = loadBalance;
    }

    public LocalBrokerConsumerService() {
        //120S 扫描一次
        final long limitMills = 2 * 60 * 1000;
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                for(Map.Entry<String, BrokerServiceEntryChannel> entry : heartbeatMap.entrySet()) {
                    String key  = entry.getKey();
                    long lastAccessTime = entry.getValue().getLastAccessTime();
                    long currentTime = System.currentTimeMillis();

                    if(currentTime - lastAccessTime > limitMills) {
                        removeByChannelId(key);
                    }
                }
            }
        }, 2 * 60, 2 * 60, TimeUnit.SECONDS);
    }

    /**
     * 根据 channelId 移除信息
     * @param channelId 通道唯一标识
     */
    private void removeByChannelId(final String channelId) {
        BrokerServiceEntryChannel channelRegister = registerMap.remove(channelId);
        log.info("移除注册信息 id: {}, channel: {}", channelId, JSON.toJSON(channelRegister));
        BrokerServiceEntryChannel channelHeartbeat = heartbeatMap.remove(channelId);
        log.info("移除心跳信息 id: {}, channel: {}", channelId, JSON.toJSON(channelHeartbeat));
    }

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
    public MQCommonResp subscribe(ConsumerSubscribeReq serviceEntry, Channel clientChannel) {
        final String channelId = ChannelUtil.getChannelId(clientChannel);
        final String topicName = serviceEntry.getTopicName();

        Set<ConsumerSubscribeBo> set = pushSubscribeMap.get(topicName);
        if(set == null) {
            set = new HashSet<>();
        }
        ConsumerSubscribeBo subscribeBo = new ConsumerSubscribeBo();
        subscribeBo.setChannelId(channelId);
        subscribeBo.setGroupName(serviceEntry.getGroupName());
        subscribeBo.setTopicName(topicName);
        subscribeBo.setTagRegex(serviceEntry.getTagRegex());
        set.add(subscribeBo);

        pushSubscribeMap.put(topicName, set);

        MQCommonResp resp = new MQCommonResp();
        resp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return resp;
    }

    @Override
    public MQCommonResp unSubscribe(ConsumerUnSubscribeReq serviceEntry, Channel clientChannel) {
        final String channelId = ChannelUtil.getChannelId(clientChannel);
        final String topicName = serviceEntry.getTopicName();

        ConsumerSubscribeBo subscribeBo = new ConsumerSubscribeBo();
        subscribeBo.setChannelId(channelId);
        subscribeBo.setGroupName(serviceEntry.getGroupName());
        subscribeBo.setTopicName(topicName);
        subscribeBo.setTagRegex(serviceEntry.getTagRegex());

        // 集合
        Set<ConsumerSubscribeBo> set = pushSubscribeMap.get(topicName);
        if(CollectionUtil.isNotEmpty(set)) {
            set.remove(subscribeBo);
        }

        MQCommonResp resp = new MQCommonResp();
        resp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return resp;
    }

    @Override
    public List<ChannelGroupNameDto> getPushSubscribeList(MQMessage mqMessage) {
        final String topicName = mqMessage.getTopic();
        Set<ConsumerSubscribeBo> set = pushSubscribeMap.get(topicName);
        if(CollectionUtil.isEmpty(set)) {
            return Collections.emptyList();
        }

        //2. 获取匹配的 tag 列表
        final String tagName = mqMessage.getTag();

        Map<String, List<ConsumerSubscribeBo>> groupMap = new HashMap<>();
        for(ConsumerSubscribeBo bo : set) {
            String tagRegex = bo.getTagRegex();

            if(ReUtil.isMatch(tagName, tagRegex)) {
                String groupName = bo.getGroupName();

                MapUtil.putToListMap(groupMap, groupName, bo);
            }
        }

        //3. 按照 groupName 分组之后，每一组只随机返回一个。最好应该调整为以 shardingkey 选择
        final String shardingKey = mqMessage.getShardingKey();
        List<ChannelGroupNameDto> channelGroupNameList = new ArrayList<>();

        for(Map.Entry<String, List<ConsumerSubscribeBo>> entry : groupMap.entrySet()) {
            List<ConsumerSubscribeBo> list = entry.getValue();
            // FIXME: 此处存在问题，多个Topic之间不能共用一个loadBalance，会导致负载不均衡（可以建立一个map）
            ConsumerSubscribeBo bo = LoadBalanceUtil.loadBalance(loadBalance, list, shardingKey);
            final String channelId = bo.getChannelId();
            BrokerServiceEntryChannel entryChannel = registerMap.get(channelId);
            if(entryChannel == null) {
                log.warn("channelId: {} 对应的通道信息为空", channelId);
                continue;
            }

            final String groupName = entry.getKey();
            ChannelGroupNameDto channelGroupNameDto = ChannelGroupNameDto.of(groupName,
                    entryChannel.getChannel());
            channelGroupNameList.add(channelGroupNameDto);
        }

        return channelGroupNameList;
    }

    @Override
    public void heartbeat(MQHeartBeatReq mqHeartBeatReq, Channel channel) {
        final String channelId = ChannelUtil.getChannelId(channel);
        log.info("[HEARTBEAT] 接收消费者心跳 {}, channelId: {}",
                JSON.toJSON(mqHeartBeatReq), channelId);

        ServiceEntry serviceEntry = new ServiceEntry();
        serviceEntry.setAddress(mqHeartBeatReq.getAddress());
        serviceEntry.setPort(mqHeartBeatReq.getPort());

        BrokerServiceEntryChannel entryChannel = InnerChannelUtil.buildEntryChannel(serviceEntry, channel);
        entryChannel.setLastAccessTime(mqHeartBeatReq.getTime());

        heartbeatMap.put(channelId, entryChannel);
    }


    /**
     * tag过滤匹配
     */
    private boolean hasMatch(List<String> tagNameList,
                             String tagRegex) {
        if(CollectionUtil.isEmpty(tagNameList)) {
            return false;
        }

        Pattern pattern = Pattern.compile(tagRegex);

        for(String tagName : tagNameList) {
            if(ReUtil.isMatch(pattern, tagName)) {
                return true;
            }
        }

        return false;
    }

}
