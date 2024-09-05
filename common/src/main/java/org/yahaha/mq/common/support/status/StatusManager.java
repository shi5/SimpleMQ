package org.yahaha.mq.common.support.status;

public class StatusManager implements IStatusManager {

    private boolean status;

    @Override
    public boolean status() {
        return this.status;
    }

    @Override
    public IStatusManager status(boolean status) {
        this.status = status;

        return this;
    }

}
