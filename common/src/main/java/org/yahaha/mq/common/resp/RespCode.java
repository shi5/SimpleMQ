package org.yahaha.mq.common.resp;

import java.io.Serializable;

public interface RespCode extends Serializable {
    String getCode();

    String getMsg();
}
