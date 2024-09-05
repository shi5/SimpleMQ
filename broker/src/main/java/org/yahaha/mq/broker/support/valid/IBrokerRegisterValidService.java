package org.yahaha.mq.broker.support.valid;

import org.yahaha.mq.broker.dto.BrokerRegisterReq;

public interface IBrokerRegisterValidService {
    /**
     * 生产者验证合法性
     * @param registerReq 注册信息
     * @return 结果
     */
    boolean producerValid(BrokerRegisterReq registerReq);

    /**
     * 消费者验证合法性
     * @param registerReq 注册信息
     * @return 结果
     */
    boolean consumerValid(BrokerRegisterReq registerReq);
}
