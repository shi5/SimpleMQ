package org.yahaha.mq.broker.persist;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.dto.persist.MQMessagePersistPut;
import org.yahaha.mq.common.constant.MessageStatusConst;
import org.yahaha.mq.common.dto.req.MQPullConsumerReq;
import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.req.component.MQMessageExt;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.dto.resp.MQPullConsumerResp;
import org.yahaha.mq.common.resp.MQCommonRespCode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
public class LocalMQBrokerPersist implements IMQBrokerPersist{
    private int newQueueNum = 4;
    // 无持久化且无清理机制，只可测试使用
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, CopyOnWriteArrayList<MQMessagePersistPut>>> consumeQueueTable;
    private final static String SEPARATOR = "@";
    private final ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable;
    private final ConcurrentHashMap<String/* topic@group@queueId */, ConcurrentSkipListMap<Long, String>> consumeStatusTable;

    public LocalMQBrokerPersist(int newQueueNum) {
        this.newQueueNum = newQueueNum;
        this.consumeQueueTable = new ConcurrentHashMap<>();
        this.offsetTable = new ConcurrentHashMap<>();
        this.consumeStatusTable = new ConcurrentHashMap<>();
    }
    
    public LocalMQBrokerPersist() {
        this.consumeQueueTable = new ConcurrentHashMap<>();
        this.offsetTable = new ConcurrentHashMap<>();
        this.consumeStatusTable = new ConcurrentHashMap<>();
    }

    @Override
    public MQCommonResp putMessage(MQMessagePersistPut mqMessage) {
        try {
            this.doPut(mqMessage);
        } catch (Exception e) {
            log.error("put mqMessage error", e);
            MQCommonResp commonResp = new MQCommonResp();
            commonResp.setRespCode(MQCommonRespCode.FAIL.getCode());
            commonResp.setRespMessage(MQCommonRespCode.FAIL.getMsg());
            return commonResp;
        }

        MQCommonResp commonResp = new MQCommonResp();
        commonResp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }

    @Override
    public MQCommonResp putMessageBatch(List<MQMessagePersistPut> putList) {
        try {
            for (MQMessagePersistPut put : putList) {
                this.doPut(put);
            }
        } catch (Exception e) {
            log.error("putBatch mqMessage error", e);
            MQCommonResp commonResp = new MQCommonResp();
            commonResp.setRespCode(MQCommonRespCode.FAIL.getCode());
            commonResp.setRespMessage(MQCommonRespCode.FAIL.getMsg());
            return commonResp;
        }

        MQCommonResp commonResp = new MQCommonResp();
        commonResp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }

    private void doPut(MQMessagePersistPut put) {
        log.info("put elem: {}", JSON.toJSON(put));

        MQMessage mqMessage = put.getMqMessage();
        final String topic = mqMessage.getTopic();
        final int queueId = put.getQueueId();

        List<MQMessagePersistPut> list = findConsumeQueue(topic, queueId);
        list.add(put);
    }

    @Override
    public int getConsumeQueueNum(String topic) {
        // 没有则创建
        if (!consumeQueueTable.containsKey(topic)) {
           createConsumeQueue(topic, this.newQueueNum);
        }
        return consumeQueueTable.get(topic).size();
    }
    
    private void createConsumeQueue(String topic, int queueNum) {
        if (!consumeQueueTable.containsKey(topic)) {
            synchronized (this) {
                if (!consumeQueueTable.containsKey(topic)) {
                    ConcurrentHashMap<Integer, CopyOnWriteArrayList<MQMessagePersistPut>> map = new ConcurrentHashMap<>();
                    for (int i = 0; i < queueNum; i++) {
                        CopyOnWriteArrayList<MQMessagePersistPut> list = new CopyOnWriteArrayList<>();
                        map.put(i, list);
                    }
                    consumeQueueTable.put(topic, map);
                }
            }
        }
    }

    private List<MQMessagePersistPut> findConsumeQueue(String topic, int queueId) {
        ConcurrentHashMap<Integer, CopyOnWriteArrayList<MQMessagePersistPut>> map = consumeQueueTable.get(topic);
        if (map == null) {
            ConcurrentHashMap<Integer, CopyOnWriteArrayList<MQMessagePersistPut>> newMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<Integer, CopyOnWriteArrayList<MQMessagePersistPut>> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        CopyOnWriteArrayList<MQMessagePersistPut> list = map.get(queueId);
        if(list == null) {
            CopyOnWriteArrayList<MQMessagePersistPut> newList = new CopyOnWriteArrayList<>();
            CopyOnWriteArrayList<MQMessagePersistPut> oldList = map.putIfAbsent(queueId, newList);
            if (oldList != null) {
                list = oldList;
            } else {
                list = newList;
            }
        }

        return list;
    }

    @Override
    public MQCommonResp updateStatus(MQConsumerUpdateStatusDto statusDto) {
        try {
            String topic = statusDto.getTopic();
            int queueId = statusDto.getQueueId();
            String group = statusDto.getConsumerGroupName();
            doUpdate(statusDto);
            doCommitOffset(topic, group, queueId);
        } catch (Exception e) {
            log.error("update status error", e);
            MQCommonResp commonResp = new MQCommonResp();
            commonResp.setRespCode(MQCommonRespCode.FAIL.getCode());
            commonResp.setRespMessage(MQCommonRespCode.FAIL.getMsg());
            return commonResp;
        }
        
        MQCommonResp commonResp = new MQCommonResp();
        commonResp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }

    @Override
    public MQCommonResp updateStatusBatch(List<MQConsumerUpdateStatusDto> statusDtoList) {
        try {
            for (MQConsumerUpdateStatusDto statusDto : statusDtoList) {
                String topic = statusDto.getTopic();
                int queueId = statusDto.getQueueId();
                String group = statusDto.getConsumerGroupName();
                doUpdate(statusDto);
                doCommitOffset(topic, group, queueId);
            }
        } catch (Exception e) {
            log.error("update status batch error", e);
            MQCommonResp commonResp = new MQCommonResp();
            commonResp.setRespCode(MQCommonRespCode.FAIL.getCode());
            commonResp.setRespMessage(MQCommonRespCode.FAIL.getMsg());
            return commonResp;
        }

        MQCommonResp commonResp = new MQCommonResp();
        commonResp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }
    
    private void doUpdate(MQConsumerUpdateStatusDto statusDto) {
        String topic = statusDto.getTopic();
        int queueId = statusDto.getQueueId();
        String messageId = statusDto.getMessageId();
        String status = statusDto.getMessageStatus();
        String group = statusDto.getConsumerGroupName();
        long offset = statusDto.getOffset();
        ConcurrentSkipListMap<Long, String> statusMap = getStatusMap(topic, group, queueId);
        String newStatus = statusMap.computeIfPresent(offset, (k, v) -> status);
        if (newStatus == null) {
            log.error("update status error, offset not found, key: {}, offset: {}",
                    topic + SEPARATOR + group + SEPARATOR + queueId, offset);
        }
    }
    
    private void doCommitOffset(String topic, String group, int queueId) {
        String offsetMapKey = topic + SEPARATOR + group;
        String statusKey = topic + SEPARATOR + group + SEPARATOR + queueId;
        ConcurrentMap<Integer, Long> offsetMap = offsetTable.get(offsetMapKey);
        ConcurrentSkipListMap<Long, String> statusMap = consumeStatusTable.get(statusKey);
        Iterator<Map.Entry<Long, String>> iterator = statusMap.entrySet().iterator();
        long offset = 0;
        while (iterator.hasNext()) {
            Map.Entry<Long, String> entry = iterator.next();
            // TODO: 消费失败了可以添加到死信队列
            // 成功或失败的消息可以删除
            if (MessageStatusConst.CONSUMER_SUCCESS.equals(entry.getValue())
                    || MessageStatusConst.CONSUMER_FAILED.equals(entry.getValue())) {
                offset = entry.getKey() + 1;
                iterator.remove(); // 使用迭代器的 remove 方法删除
            }
        }
        final long finalOffset = offset;
        Long result = offsetMap.computeIfPresent(queueId, (k, v) -> finalOffset > v ? finalOffset : v);
        if (result == finalOffset) {
            log.info("commit offset, topic: {}, group: {}, queueId: {}, offset: {}", topic, group, queueId, finalOffset);
        }
    }
    
    private ConcurrentSkipListMap<Long, String> getStatusMap(String topic, String group, int queueId) {
        String key = topic + SEPARATOR + group + SEPARATOR + queueId;
        ConcurrentSkipListMap<Long, String> statusMap = consumeStatusTable.get(key);
        if (statusMap == null) {
            ConcurrentSkipListMap<Long, String> newMap = new ConcurrentSkipListMap<>();
            ConcurrentSkipListMap<Long, String> oldMap = consumeStatusTable.putIfAbsent(key, newMap);
            if (oldMap != null) {
                statusMap = oldMap;
            } else {
                statusMap = newMap;
            }
        }
        return statusMap;
    }
    
    private long getConsumeOffset(String topic, String group, int queueId) {
        String key = topic + SEPARATOR + group;
        ConcurrentMap<Integer, Long> offsetMap = offsetTable.get(key);
        if (offsetMap == null) {
            ConcurrentMap<Integer, Long> newMap = new ConcurrentHashMap<>();
            ConcurrentMap<Integer, Long> oldMap = offsetTable.putIfAbsent(key, newMap);
            if (oldMap != null) {
                offsetMap = oldMap;
            } else {
                offsetMap = newMap;
            }
        }
        return offsetMap.computeIfAbsent(queueId, k -> 0L);
    }
    
    private void commitConsumeOffset(String topic, String group, int queueId, long offset) {
        if (offset < 0) {
            return;
        }
        String key = topic + SEPARATOR + group;
        ConcurrentMap<Integer, Long> offsetMap = offsetTable.get(key);
        if (offsetMap == null) {
            ConcurrentMap<Integer, Long> newMap = new ConcurrentHashMap<>();
            ConcurrentMap<Integer, Long> oldMap = offsetTable.putIfAbsent(key, newMap);
            if (oldMap != null) {
                offsetMap = oldMap;
            } else {
                offsetMap = newMap;
            }
        } 
        offsetMap.put(queueId, offset);
    }

    @Override
    public MQPullConsumerResp pull(MQPullConsumerReq pullReq, Channel channel) {
        String topic = pullReq.getTopic();
        String group = pullReq.getGroupName();
        int queueId = ThreadLocalRandom.current().nextInt(getConsumeQueueNum(topic));
        int pullSize = pullReq.getSize();
        String tagRegex = pullReq.getTagRegex();
        List<MQMessagePersistPut> consumeQueue = findConsumeQueue(topic, queueId);
        ConcurrentSkipListMap<Long, String> statusMap = getStatusMap(topic, group, queueId);
        List<MQMessageExt> resultList = new ArrayList<>();
        final long startOffset = getConsumeOffset(topic, group, queueId);
        long queueMaxOffset = consumeQueue.size();
        long currOffset = startOffset;
        while (currOffset < queueMaxOffset) {
            MQMessagePersistPut put = consumeQueue.get((int) startOffset);
            MQMessage mqMessage = put.getMqMessage();
            if (mqMessage.getTag().matches(tagRegex) || isMessageNotConsumed(topic, group, queueId, currOffset)) {
                MQMessageExt messageExt = new MQMessageExt(currOffset, queueId, mqMessage);
                resultList.add(messageExt);
                statusMap.put(currOffset, MessageStatusConst.TO_CONSUMER_SUCCESS);
            }
            currOffset++;
            if (resultList.size() >= pullSize) {
                break;
            }
        }
        
        MQPullConsumerResp resp = new MQPullConsumerResp(resultList);
        resp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        resp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return resp;
    }
    
    private boolean isMessageNotConsumed(String topic, String group, int queueId, long offset) {
        ConcurrentSkipListMap<Long, String> statusMap = getStatusMap(topic, group, queueId);
        return statusMap == null && !statusMap.containsKey(offset);
    }
}