package org.yahaha.mq.broker.dto;

import lombok.Data;
import org.yahaha.mq.common.rpc.RpcAddress;

@Data
public class ServiceEntry extends RpcAddress {

    /**
     * 分组名称
     */
    private String groupName;
}
