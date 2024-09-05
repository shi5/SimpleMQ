package org.yahaha.mq.broker.persist;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.dto.persist.MQMessagePersistPut;
import org.yahaha.mq.broker.persist.constant.AppendMessageStatus;
import org.yahaha.mq.broker.persist.constant.CommitLogUnit;
import org.yahaha.mq.common.support.thread.ThreadFactoryImpl;
import org.yahaha.mq.common.dto.req.component.MQMessage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class CommitLog {
    // 消息魔数 daa320a7
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    // 文件结束魔数 cbd43194
    protected final static int BLANK_MAGIC_CODE = -875286124;
    protected final MappedFileQueue mappedFileQueue;
    protected final DefaultMQBrokerPersist defaultMQBrokerPersist;
    protected HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);

    protected final AppendMessageCallback appendMessageCallback;

    protected final CommitLogFlushThreadPool commitLogFlushExecutor;

    protected final ReentrantLock putMessageLock;

    public CommitLog(final DefaultMQBrokerPersist defaultMQBrokerPersist) {
        this.defaultMQBrokerPersist = defaultMQBrokerPersist;
        this.mappedFileQueue = new MappedFileQueue(defaultMQBrokerPersist.getStorePathCommitLog(),
                defaultMQBrokerPersist.getMappedFileSizeCommitLog());
        this.putMessageLock = new ReentrantLock();
        this.appendMessageCallback = new AppendMessageCallback();
        this.commitLogFlushExecutor = new CommitLogFlushThreadPool(12, "FlushCommitLogThread_");
    }

    // TODO: 加载之后需要进行验证（recover）
    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public boolean putMessage(final MQMessagePersistPut put) {
        String topic = put.getTopic();
        int queueId = put.getQueueId();
        AppendMessageResult result = null;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        putMessageLock.lock();
        try{
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            }
            if (null == mappedFile) {
                log.error("create mapped file error, maybe disk full");
                return false;
            }
            result = mappedFile.appendMessage(put, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        log.error("create mapped file error, maybe disk full");
                        return false;
                    }
                    result = mappedFile.appendMessage(put, this.appendMessageCallback);
                    break;
                default:
                    return false;
            }
        } finally {
            putMessageLock.unlock();
        }

        // TODO: 需要考虑commitlog成功，但是consumequeue失败的情况
        ConsumeQueue consumeQueue = this.defaultMQBrokerPersist.findConsumeQueue(topic, queueId);
        MappedFile mappedFileCQ = null;
        if (null == consumeQueue) {
            log.error("consume queue not found, topic: {}, queueId: {}", topic, queueId);
        } else {
            mappedFileCQ = consumeQueue.putMessagePositionInfo(result.getWroteOffset(), result.getWroteBytes(),
                    put.getMqMessage().getTag().hashCode() , result.getLogicsOffset());
        }


        log.info("put message to commit log, topic: {}, queueId: {}, offset: {}, size: {}"
                , topic, queueId, result.getWroteOffset(), result.getWroteBytes());

        // 处理刷盘
        handleDiskFlush(mappedFile, mappedFileCQ);

        return true;
    }



    private void handleDiskFlush(MappedFile mappedFile, MappedFile mappedFileCQ) {
        if (defaultMQBrokerPersist.isSyncFlush()) {
            mappedFile.flush();
            if (mappedFileCQ != null){
                mappedFileCQ.flush();
            }
        } else {
            this.commitLogFlushExecutor.submitFlushTask(new Runnable() {
                @Override
                public void run() {
                    mappedFile.flush();
                    if (mappedFileCQ != null){
                        mappedFileCQ.flush();
                    }
                }
            });
        }
        return;
    }

    /**
     * 1. 清理过期消息
     * 2. 根据offset获取消息
     * 3. 最小offset
     * 4. 最大offset
     */

    // 计算消息长度
    protected static int calMsgLength(int bodyLength, int topicLength, int tagLength) {
        final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //QUEUEID
                + 4 + bodyLength //BODY
                + 1 + topicLength //TOPIC
                + 1 + tagLength; //TAG
        return msgLen;
    }

    /**
     *  计算存入CommitLog的消息长度
     * @param length 整个MQMessage的长度
     * @return
     */
    protected static int calMsgLength(int length) {
        final int msgLen = 4 //TOTALSIZE
                + 4 //MAGICCODE
                + 4 //QUEUEID
                + 4 //MESSAGELENGTH
                + length; //MESSAGE
        return msgLen;
    }

    public void recover(long maxPhyOffsetOfConsumeQueue) {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0)
                index = 0;

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

            // processOffset为CommitLog文件已确认的物理偏移量，
            // 等于mappedFile.getFileFromOffset加上mappedFileOffset
            long processOffset = mappedFile.getFileFromOffset();
            // 为当前文件已校验通过的物理偏移量
            long mappedFileOffset = 0;

            while (true) {
                int size = this.checkMessageAndReturnSize(byteBuffer);
                if (size > 0) {
                    mappedFileOffset += size;
                } else if (size == 0) {
                    index++;
                    // 没有下一个文件了，跳出循环
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                        // 如果还有下一个文件，则重置processOffset、mappedFileOffset并重复上述步骤
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }else {
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }
            processOffset += mappedFileOffset;
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMQBrokerPersist.truncateDirtyLogicFiles(processOffset);
            }
        } else {
            this.defaultMQBrokerPersist.destroyLogics();
        }
    }

    private int checkMessageAndReturnSize(ByteBuffer byteBuffer) {
        try{
            int totalSize = byteBuffer.getInt();
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return 0;
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return -1;
            }
            int queueId = byteBuffer.getInt();
            int bodyLength = byteBuffer.getInt();
            byteBuffer.position(byteBuffer.position() + bodyLength);
            int readLength = calMsgLength(bodyLength);
            if (readLength != totalSize) {
                log.error(
                        "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLength={}",
                        totalSize, readLength, bodyLength);
                return -1;
            }
            return totalSize;
        } catch (Exception e) {
        }
        return -1;
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset();
        }

        return -1;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public CommitLogUnit getUnitByOffset(long offset, int size) {
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
        if (mappedFile == null) {
            return null;
        }
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        byteBuffer.position((int) (offset % this.defaultMQBrokerPersist.getMappedFileSizeCommitLog()));
        int totalSize = byteBuffer.getInt();
        int magicCode = byteBuffer.getInt();
        int queueId = byteBuffer.getInt();
        int bodyLength = byteBuffer.getInt();
        byte[] bodyData = new byte[bodyLength];
        byteBuffer.get(bodyData);
        MQMessage msg = JSON.parseObject(new String(bodyData, StandardCharsets.UTF_8), MQMessage.class);
        return new CommitLogUnit(totalSize, magicCode, queueId, bodyLength, msg);
    }


    class AppendMessageCallback {
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        private final ByteBuffer msgIdMemory;
        private final ByteBuffer msgStoreItemMemory;
        private final int maxMessageSize = 1024 * 1024 * 4; //默认消息最大长度4M
        private final StringBuilder keyBuilder = new StringBuilder();
        private final StringBuilder msgIdBuilder = new StringBuilder();

        AppendMessageCallback() {
            this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
            this.msgStoreItemMemory = ByteBuffer.allocate(maxMessageSize + END_FILE_MIN_BLANK_LENGTH);
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
                                            final int maxBlank, final MQMessagePersistPut put) {
            long wroteOffset = fileFromOffset + byteBuffer.position();
            keyBuilder.setLength(0);
            keyBuilder.append(put.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(put.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            MQMessage msg = put.getMqMessage();
            final byte[] bodyData = JSON.toJSONString(msg).getBytes(StandardCharsets.UTF_8);
            final int bodyLength = bodyData == null ? 0 : bodyData.length;
            final int msgLength = calMsgLength(bodyLength);
            if (msgLength > this.maxMessageSize) {  // 最大消息长度 65536
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLength + ", msg body size: " + bodyLength
                        + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // 如果文件放不下了就返回END_OF_FILE
            if ((msgLength + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                this.msgStoreItemMemory.putInt(maxBlank);
                this.msgStoreItemMemory.putInt(BLANK_MAGIC_CODE);
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE);
            }

            this.resetByteBuffer(msgStoreItemMemory, msgLength);
            this.msgStoreItemMemory.putInt(msgLength);
            this.msgStoreItemMemory.putInt(MESSAGE_MAGIC_CODE);
            this.msgStoreItemMemory.putInt(put.getQueueId());
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0)
                this.msgStoreItemMemory.put(bodyData);

            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLength);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, queueOffset, msgLength);
            CommitLog.this.topicQueueTable.put(key, ++queueOffset);

            return result;
        }
        

//        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
//                                            final int maxBlank, final MQMessagePersistPut put) {
//            long wroteOffset = fileFromOffset + byteBuffer.position();
//            keyBuilder.setLength(0);
//            keyBuilder.append(put.getTopic());
//            keyBuilder.append('-');
//            keyBuilder.append(put.getQueueId());
//            String key = keyBuilder.toString();
//            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
//            if (null == queueOffset) {
//                queueOffset = 0L;
//                CommitLog.this.topicQueueTable.put(key, queueOffset);
//            }
//
//            MQMessage msg = put.getMqMessage();
//            final byte[] bodyData = msg.getPayload().getBytes();
//            final int bodyLength = msg.getPayload() == null ? 0 : bodyData.length;
//            final byte[] topicData =
//                    msg.getTopic().getBytes() == null ? null : msg.getTopic().getBytes(StandardCharsets.UTF_8);
//            final int topicLength = topicData == null ? 0 : topicData.length;
//            final byte[] tagData = msg.getTag().getBytes(StandardCharsets.UTF_8);
//            final int tagLength = tagData == null ? 0 : tagData.length;
//            final int msgLength = calMsgLength(bodyLength, topicLength, tagLength);
//            if (msgLength > this.maxMessageSize) {  // 最大消息长度 65536
//                CommitLog.log.warn("message size exceeded, msg total size: " + msgLength + ", msg body size: " + bodyLength
//                        + ", maxMessageSize: " + this.maxMessageSize);
//                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
//            }
//
//            // 如果文件放不下了就返回END_OF_FILE
//            if ((msgLength + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
//                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
//                this.msgStoreItemMemory.putInt(maxBlank);
//                this.msgStoreItemMemory.putInt(BLANK_MAGIC_CODE);
//                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);
//                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE);
//            }
//
//            this.resetByteBuffer(msgStoreItemMemory, msgLength);
//            this.msgStoreItemMemory.putInt(msgLength);
//            this.msgStoreItemMemory.putInt(MESSAGE_MAGIC_CODE);
//            this.msgStoreItemMemory.putInt(put.getQueueId());
//            this.msgStoreItemMemory.putInt(bodyLength);
//            if (bodyLength > 0)
//                this.msgStoreItemMemory.put(bodyData);
//            this.msgStoreItemMemory.put((byte) topicLength);
//            this.msgStoreItemMemory.put(topicData);
//            this.msgStoreItemMemory.put((byte) tagLength);
//            if (tagLength > 0)
//                this.msgStoreItemMemory.put(tagData);
//
//            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLength);
//            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, queueOffset, msgLength);
//            CommitLog.this.topicQueueTable.put(key, ++queueOffset);
//
//            return result;
//        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }
    }

    class CommitLogFlushThreadPool {
        private final ExecutorService executorService;

        public CommitLogFlushThreadPool(int poolSize, String baseName) {
            ThreadFactory threadFactory = new ThreadFactoryImpl(baseName);
            this.executorService = new ThreadPoolExecutor(
                    poolSize, // corePoolSize
                    2 * poolSize, // maximumPoolSize
                    1000L, TimeUnit.MILLISECONDS, // keepAliveTime
                    new LinkedBlockingQueue<>(), // workQueue
                    threadFactory// threadFactory
            );
        }

        public void submitFlushTask(Runnable task) {
            this.executorService.submit(task);
        }

        public void shutdown() {
            this.executorService.shutdown();
            try {
                if (!this.executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    this.executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                this.executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
