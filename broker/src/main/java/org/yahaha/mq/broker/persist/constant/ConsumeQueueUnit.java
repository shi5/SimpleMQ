package org.yahaha.mq.broker.persist.constant;

public class ConsumeQueueUnit {
    private long offset;
    private int size;
    private long tagCode;
    public ConsumeQueueUnit(long offset, int size, long tagCode) {
        this.offset = offset;
        this.size = size;
        this.tagCode = tagCode;
    }

    public ConsumeQueueUnit() {
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getTagCode() {
        return tagCode;
    }

    public void setTagCode(long tagCode) {
        this.tagCode = tagCode;
    }
}
