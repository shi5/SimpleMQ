package org.yahaha.mq.common.support.status;

public interface IStatusManager {
    /**
     * 获取状态编码
     * @return 状态编码
     */
    boolean status();

    /**
     * 设置状态编码
     * @param status 编码
     * @return this
     */
    IStatusManager status(final boolean status);
}
