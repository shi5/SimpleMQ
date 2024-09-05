package org.yahaha.mq.broker.store;

import org.yahaha.mq.broker.dto.persist.MQMessagePersistPut;
import org.yahaha.mq.broker.persist.DefaultMQBrokerPersist;
import org.yahaha.mq.common.dto.req.component.MQMessage;

public class TestDefaultMQBrokerPersist {
    public static void main(String[] args) {
        DefaultMQBrokerPersist persist = new DefaultMQBrokerPersist();
        for (int i = 0; i < 5; i++){
            MQMessage msg = new MQMessage();
            msg.setGroupName("TEST_CONSUMER");
            msg.setTopic("TEST");
            msg.setTag("TAGA");
            msg.setPayload("HELLO" + i);
            msg.setTraceId("123456");
            msg.setMethodType("PERSIST_PUT");
            MQMessagePersistPut put = new MQMessagePersistPut();
            put.setTopic("TEST");
            put.setQueueId(0);
            put.setMqMessage(msg);
            persist.putMessage(put);
        }
    } 
}
