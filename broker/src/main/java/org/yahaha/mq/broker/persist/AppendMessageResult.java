package org.yahaha.mq.broker.persist;

import org.yahaha.mq.broker.persist.constant.AppendMessageStatus;

public class AppendMessageResult {
    private AppendMessageStatus status;
    private long wroteOffset;   // 写入commitlog的物理偏移量
    private long logicsOffset;  // 写入consumequeue的逻辑偏移量
    private int wroteBytes;    // 写入字节数

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public long getLogicsOffset() {
        return logicsOffset;
    }

    public void setLogicsOffset(long logicsOffset) {
        this.logicsOffset = logicsOffset;
    }

    public AppendMessageResult(AppendMessageStatus status) {
        this.status = status;
    }
    public AppendMessageResult(AppendMessageStatus status, int wroteBytes) {
        this.status = status;
        this.wroteBytes = wroteBytes;
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, long logicsOffset, int wroteBytes) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.logicsOffset = logicsOffset;
        this.wroteBytes = wroteBytes;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }
    public AppendMessageStatus getStatus() {
        return status;
    }

    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }
}
