package org.yahaha.mq.common.support.hook;

import lombok.extern.slf4j.Slf4j;

/**
 * rpc 关闭 hook
 * （1）可以添加对应的 hook 管理类
 */
@Slf4j
public abstract class AbstractShutdownHook implements RpcShutdownHook {

    @Override
    public void hook() {
        log.info("[Shutdown Hook] start");
        this.doHook();
        log.info("[Shutdown Hook] end");
    }

    /**
     * 执行 hook 操作
     *      */
    protected abstract void doHook();

}
