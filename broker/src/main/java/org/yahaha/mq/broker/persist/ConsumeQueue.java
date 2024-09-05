package org.yahaha.mq.broker.persist;

import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.persist.constant.ConsumeQueueUnit;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

@Slf4j
public class ConsumeQueue {
    public static final int CQ_STORE_UNIT_SIZE = 20;
    protected final DefaultMQBrokerPersist defaultMQBrokerPersist;

    protected final MappedFileQueue mappedFileQueue;
    private final String topic;
    private final int queueId;
    private final ByteBuffer byteBufferIndex;
    
    private final String storePath;
    private final int mappedFileSize;
    private long maxPhysicOffset = -1;
    private volatile long minLogicOffset = 0;   // 逻辑队列的最小offset, 对应于现有CommitLog文件最小offset

    public ConsumeQueue(
            final String topic,
            final int queueId,
            final String storePath,
            final int mappedFileSize,
            final DefaultMQBrokerPersist defaultMQBrokerPersist) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMQBrokerPersist = defaultMQBrokerPersist;

        this.topic = topic;
        this.queueId = queueId;

        String queueDir = this.storePath
                + File.separator + topic
                + File.separator + queueId;

        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize);

        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load ConsumeQueue " + topic + "-" + queueId + (result ? " OK" : " Failed"));
        return result;
    }

    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (mappedFiles.isEmpty()) {
            return;
        }
        int index = mappedFiles.size() - 3;
        if (index < 0)
            index = 0;

        int mappedFileSizeLogics = this.mappedFileSize;
        MappedFile mappedFile = mappedFiles.get(index);
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        long processOffset = mappedFile.getFileFromOffset();
        long mappedFileOffset = 0;
        while (true) {
            for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                long tagsCode = byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                    this.maxPhysicOffset = offset + size;
                } else {
                    log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                            + offset + " " + size + " " + tagsCode);
                    break;
                }
            }

            if (mappedFileOffset == mappedFileSizeLogics) {
                index++;
                if (index >= mappedFiles.size()) {

                    log.info("recover last consume queue file over, last mapped file "
                            + mappedFile.getFileName());
                    break;
                } else {
                    mappedFile = mappedFiles.get(index);
                    byteBuffer = mappedFile.sliceByteBuffer();
                    processOffset = mappedFile.getFileFromOffset();
                    mappedFileOffset = 0;
                    log.info("recover next consume queue file, " + mappedFile.getFileName());
                }
            } else {
                log.info("recover current consume queue file over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                break;
            }
        }

        processOffset += mappedFileOffset;
        this.mappedFileQueue.truncateDirtyFiles(processOffset);
    }

    public void truncateDirtyLogicFiles(long phyOffset) {
        int logicFileSize = this.mappedFileSize;
        this.maxPhysicOffset = phyOffset;
        while(true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
                
                mappedFile.setWrotePosition(0);
                mappedFile.setFlushedPosition(0);
                
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();
                    
                    if (i == 0) {
                        if(offset >= phyOffset) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        }else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                        }
                    } else {
                        if (offset >= 0 && size > 0) {
                            // 如果文件的偏移量大于offset则直接返回
                            if (offset >= phyOffset) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            // 设置maxPhysicOffset
                            this.maxPhysicOffset = offset + size;
                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            }
        }
    }

    public MappedFile putMessagePositionInfo(final long offset, final int size, final long tagsCode,
                                           final long cqOffset) {
        if (offset + size <= this.maxPhysicOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return null;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {
            // 判断mappedFile 是否是第一个 且 cqOffset 不是0 且mappedFile 写位置是0
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                // 设置 最小offset
                this.minLogicOffset = expectLogicOffset;

                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                        + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                // 当前在这个consumeQueue 的offset
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();
                // 你现在要插入offset 比当前在这个consumeQueue 的offset要小，这个就是说明 你在找之前的位置插入，但是人家已经有东西了
                // 要是让你插入的话 就会造成重复，所以这里不让你插入的
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return null;
                }

                // 按照正常情况下是一样大的，不一样大打印错误日志
                if (expectLogicOffset != currentLogicOffset) {
                    log.warn(
                            "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset,
                            currentLogicOffset,
                            this.topic,
                            this.queueId,
                            expectLogicOffset - currentLogicOffset
                    );
                }
            }
            // 设置最大的 物理offset
            this.maxPhysicOffset = offset + size;
            boolean result = mappedFile.appendMessage(this.byteBufferIndex.array());
            // 刷盘
            if (result) {
                return mappedFile;
            }
        }
        return null;
    }

    private void fillPreBlank(MappedFile mappedFile, long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    public long getMaxPhysicOffset() {
        return this.maxPhysicOffset;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
    }

    public String getTopic() {
        return topic;
    }
    
    public int getQueueId() {
        return queueId;
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void correctMinOffset(long phyMinOffset) {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            ByteBuffer byteBuffer = mappedFile.selectMappedBuffer(0);
            if (byteBuffer != null) {
                try {
                    int remaining = byteBuffer.remaining();
                    for (int i = 0; i < remaining; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetPy = byteBuffer.getLong();
                        byteBuffer.getInt();
                        long tagsCode = byteBuffer.getLong();

                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = mappedFile.getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                    this.getMinOffsetInQueue(), this.topic, this.queueId);
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                }
            }
        }
    }

    private long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    public ConsumeQueueUnit getUnit(final long cqOffset) {
        long logicOffset = cqOffset * CQ_STORE_UNIT_SIZE;
        if (logicOffset < this.minLogicOffset) {
            return null;
        }
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(logicOffset);
        if (mappedFile != null) {
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position((int) (logicOffset - mappedFile.getFileFromOffset()));
            long offset = byteBuffer.getLong();
            int size = byteBuffer.getInt();
            long tagsCode = byteBuffer.getLong();
            return new ConsumeQueueUnit(offset, size, tagsCode);
        }
        return null;
    }
}
