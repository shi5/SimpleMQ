package org.yahaha.mq.broker.persist;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.dto.persist.MQMessagePersistPut;
import org.yahaha.mq.broker.persist.constant.CommitLogUnit;
import org.yahaha.mq.broker.persist.constant.ConsumeQueueUnit;
import org.yahaha.mq.common.constant.MessageStatusConst;
import org.yahaha.mq.common.dto.req.MQPullConsumerReq;
import org.yahaha.mq.common.dto.req.component.MQConsumerUpdateStatusDto;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.dto.req.component.MQMessageExt;
import org.yahaha.mq.common.dto.resp.MQCommonResp;
import org.yahaha.mq.common.dto.resp.MQPullConsumerResp;
import org.yahaha.mq.common.resp.MQCommonRespCode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.*;
import java.util.concurrent.*;

@Slf4j
public class DefaultMQBrokerPersist implements IMQBrokerPersist{
    private boolean syncFlush = false;
    private int newQueueNum = 4;
//    private String storePathRootDir = System.getProperty("user.home") + File.separator + "store";
//    private String storePathCommitLog = System.getProperty("user.home") + File.separator + "store"
//            + File.separator + "commitlog";
    private String storePathRootDir = "D:\\Temp" + File.separator + "store";
    private String storePathCommitLog = "D:\\Temp" + File.separator + "store"
            + File.separator + "commitlog";
    private String storePathConsumeOffset = "D:\\Temp" + File.separator + "store"
            + File.separator + "consumeoffset.json";
    // CommitLog file size,default is 1G
//    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;
    private int mappedFileSizeCommitLog = 1024 * 1024 * 10;
    // ConsumeQueue file size,default is 30W
    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;

    private final CommitLog commitLog;

    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable ;
    private final static String SEPARATOR = "@";
    private ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>> offsetTable;
    private final ConcurrentHashMap<String/* topic@group@queueId */, ConcurrentSkipListMap<Long, String>> consumeStatusTable;
    

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public DefaultMQBrokerPersist() {
        if (!new File(storePathRootDir).exists()) {
            new File(storePathRootDir).mkdirs();
        }
        this.commitLog = new CommitLog(this);
        this.consumeQueueTable = new ConcurrentHashMap<String, ConcurrentMap<Integer, ConsumeQueue>>();
        this.offsetTable = new ConcurrentHashMap<String, ConcurrentMap<Integer, Long>>();
        this.consumeStatusTable = new ConcurrentHashMap<>();
        boolean result = this.load();
        if (result) {
            log.info("load persist file OK");
            // TODO： 关闭时应该先将ConsumeOffset持久化
            scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    persistConsumeOffset();
                }
            }, 5, 5, TimeUnit.SECONDS);
        } else {
            log.error("load persist file failed");
        }
    }

    @Override
    public synchronized MQCommonResp putMessage(MQMessagePersistPut put) {
        // TODO: 存储失败处理
        this.doPut(put);

        MQCommonResp commonResp = new MQCommonResp();
        commonResp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return commonResp;
    }

    private boolean doPut(MQMessagePersistPut put) {
        return commitLog.putMessage(put);
    }

    @Override
    public MQCommonResp putMessageBatch(List<MQMessagePersistPut> putList) {
        // FIXME: 这样批量会有问题，可能会有部分刷盘成功，部分失败的情况
        for(MQMessagePersistPut put : putList) {
            this.doPut(put);
        }

        MQCommonResp commonResp = new MQCommonResp();
        commonResp.setRespCode(MQCommonRespCode.SUCCESS.getCode());
        commonResp.setRespMessage(MQCommonRespCode.SUCCESS.getMsg());
        return commonResp;
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
                    ConcurrentHashMap<Integer, ConsumeQueue> map = new ConcurrentHashMap<>();
                    for (int i = 0; i < queueNum; i++) {
                        ConsumeQueue consumeQueue = new ConsumeQueue(
                                topic,
                                i,
                                storePathRootDir + File.separator + "consumequeue",
                                this.mappedFileSizeConsumeQueue,
                                this);
                        map.put(i, consumeQueue);
                    }
                    consumeQueueTable.put(topic, map);
                }
            }
        }
    }

    // TODO： 需要添加recover，验证准确性
    public boolean load() {
        boolean result = this.commitLog.load();
        result = result && this.loadConsumeQueue();
        result = result && this.loadConsumeOffset();
        if (result) {
            recover();
        }
        return result;
    }

    public void recover() {
        long maxPhyOffsetOfConsumeQueue = recoverConsumeQueue();
        this.commitLog.recover(maxPhyOffsetOfConsumeQueue);
        this.recoverTopicQueueTable();
    }

    private void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQueue());
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }

    private long recoverConsumeQueue() {
        long maxPhysicOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }

        return maxPhysicOffset;
    }


    private boolean loadConsumeOffset() {
        File file = new File(storePathConsumeOffset);
        if (!file.exists()) {
            return true;
        }
        try {
            byte[] data = new byte[(int) file.length()];
            boolean result;
            try(FileInputStream fileInputStream = new FileInputStream(file)) {
                int len = fileInputStream.read(data);
                result = len == data.length;
            }
            if (!result) {
                return false;
            }
            String json = new String(data);
            offsetTable = JSON.parseObject(json, new TypeReference<ConcurrentMap<String/* topic@group */, ConcurrentMap<Integer, Long>>>(){});
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private void persistConsumeOffset() {
        try {
            String json = JSON.toJSONString(offsetTable, SerializerFeature.PrettyFormat);
            try (FileOutputStream fileOutputStream = new FileOutputStream(storePathConsumeOffset)) {
                fileOutputStream.write(json.getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private boolean loadConsumeQueue() {
        File dirLogic = new File(storePathRootDir + File.separator + "consumequeue");
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        ConsumeQueue logic = new ConsumeQueue(
                                topic,
                                queueId,
                                storePathRootDir + File.separator + "consumequeue",
                                this.mappedFileSizeConsumeQueue,
                                this);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMQBrokerPersist.this.consumeQueueTable;

        // 遍历
        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                // todo
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic = new ConsumeQueue(
                    topic,
                    queueId,
                    storePathRootDir + File.separator + "consumequeue",
                    this.mappedFileSizeConsumeQueue,
                    this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                logic = newLogic;
            }
        }

        return logic;
    }

    public boolean isSyncFlush() {
        return syncFlush;
    }


    public void setSyncFlush(Boolean syncFlush) {
        this.syncFlush = syncFlush;
    }

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }

    public int getMappedFileSizeCommitLog() {
        return mappedFileSizeCommitLog;
    }

    public void setMappedFileSizeCommitLog(int mappedFileSizeCommitLog) {
        this.mappedFileSizeCommitLog = mappedFileSizeCommitLog;
    }

    public int getMappedFileSizeConsumeQueue() {
        return mappedFileSizeConsumeQueue;
    }

    public void setMappedFileSizeConsumeQueue(int mappedFileSizeConsumeQueue) {
        this.mappedFileSizeConsumeQueue = mappedFileSizeConsumeQueue;
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
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        return offsetMap.computeIfAbsent(queueId, k -> consumeQueue.getMaxOffsetInQueue());
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
    public MQPullConsumerResp pull(final MQPullConsumerReq pullReq, final Channel channel) {
        String topic = pullReq.getTopic();
        String group = pullReq.getGroupName();
        int queueId = ThreadLocalRandom.current().nextInt(getConsumeQueueNum(topic));
        int pullSize = pullReq.getSize();
        String tagRegex = pullReq.getTagRegex();
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        ConcurrentSkipListMap<Long, String> statusMap = getStatusMap(topic, group, queueId);
        List<MQMessageExt> resultList = new ArrayList<>();
        final long startOffset = getConsumeOffset(topic, group, queueId);
        long queueMaxOffset = consumeQueue.getMaxOffsetInQueue();
        long currOffset = startOffset;
        while (currOffset < queueMaxOffset) {
            MQMessage mqMessage = getMessageByOffset(topic, queueId, currOffset);
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
    
    private MQMessage getMessageByOffset(String topic, int queueId, long startOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        ConsumeQueueUnit consumeQueueUnit = consumeQueue.getUnit(startOffset);
        CommitLogUnit commitLogUnit = commitLog.getUnitByOffset(consumeQueueUnit.getOffset(), consumeQueueUnit.getSize());
        return commitLogUnit.getMsg();
    }

    private boolean isMessageNotConsumed(String topic, String group, int queueId, long offset) {
        ConcurrentSkipListMap<Long, String> statusMap = getStatusMap(topic, group, queueId);
        return statusMap == null && !statusMap.containsKey(offset);
    }


    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }
}
