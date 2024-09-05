package org.yahaha.mq.broker.persist;

import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.broker.dto.persist.MQMessagePersistPut;
import org.yahaha.mq.broker.persist.constant.AppendMessageStatus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MappedFile {
    public static final int OS_PAGE_SIZE = 1024 * 4;

    // 当前文件的写指针，从0开始（内存映射文件中的写指针）
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    // 将该指针之前的数据持久化存储到磁盘中
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    // 文件大小
    protected int fileSize;

    // 文件通道
    protected FileChannel fileChannel;
    // 文件名称
    private String fileName;
    // 该文件的初始偏移量
    private long fileFromOffset;

    // 物理文件
    private File file;
    // 物理文件对应的内存映射Buffer
    private MappedByteBuffer mappedByteBuffer;
    // 是否是MappedFileQueue队列中第一个文件。
    private boolean firstCreateInQueue = false;

    public MappedFile(final String fileName, final int fileSize)  throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        //确保父目录存在
        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file,"rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public AppendMessageResult appendMessage(MQMessagePersistPut put, CommitLog.AppendMessageCallback cb) {
        int currentPos = this.wrotePosition.get();
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, put);
            this.wrotePosition.addAndGet(result.getWroteBytes());
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }


    // 针对异步刷盘使用
    public int commit(final int commitLeastPages ) {
        return 0;
    }

    //刷盘
    public int flush() {
        if (getReadPosition() > getFlushedPosition()) {
            // todo flushedPosition应该等于MappedByteBuffer中的写指针
            int value = getReadPosition();

            try {
                //We only append data to fileChannel or mappedByteBuffer, never both.
                if (this.fileChannel.position() != 0) {
                    // todo 将内存中数据持久化到磁盘
                    this.fileChannel.force(false);
                } else {
                    //
                    this.mappedByteBuffer.force();
                }
            } catch (Throwable e) {
                log.error("Error occurred when force data to disk.", e);
            }
            // 设置 记录flush位置
            this.flushedPosition.set(value);
        }
        // todo 返回flush位置
        return this.getFlushedPosition();
    }

//    // 预热
//    public void warmMappedFile(FlushDiskType type, int pages) {
//
//    }

    public boolean cleanup() {
        return false;
    }

    // 文件是否已写满
    public boolean isFull() {
        // 文件大小是否与写入数据位置相等
        return this.fileSize == this.wrotePosition.get();
    }

    public int getReadPosition() {
        // 如果writeBuffer为空使用写入位置，否则使用提交位置
        return this.wrotePosition.get();
    }

    public int getWrotePosition() {
        return this.wrotePosition.get();
    }

    public int getFlushedPosition() {
        return this.flushedPosition.get();
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    public String getFileName() {
        return fileName;
    }

    public boolean appendMessage(final byte[] data) {
        // 获取当前写的位置
        int currentPos = this.wrotePosition.get();

        // 判断在这个MappedFile中能不能 放下
        if ((currentPos + data.length) <= this.fileSize) {
            try {
                // 写入消息
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            // 重置 写入消息
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }


    public void destroy() {
        if (this.fileChannel != null) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");
                boolean result = this.file.delete();
                log.info("delete file " + this.fileName + (result ? " OK" : " Failed"));
            } catch (IOException e) {
                log.error("close file channel " + this.fileName + " Failed. ", e);
            }
        }
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }
    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public ByteBuffer selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
            byteBuffer.position(pos);
            int size = readPosition - pos;
            ByteBuffer byteBufferNew = byteBuffer.slice();
            byteBufferNew.limit(size);
            return byteBufferNew;
        }
        
        return null;
    }
}
