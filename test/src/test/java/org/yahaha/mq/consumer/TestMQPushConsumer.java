package org.yahaha.mq.consumer;

import com.alibaba.fastjson.JSON;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.resp.ConsumerStatus;
import org.yahaha.mq.consumer.api.IMQConsumerListener;
import org.yahaha.mq.consumer.api.IMQConsumerListenerContext;

public class TestMQPushConsumer {
    public static void main(String[] args) {
        final MQPushConsumer mqConsumerPush = new MQPushConsumer();
        mqConsumerPush.start();

        mqConsumerPush.subscribe("TOPIC", "TAGA");
        mqConsumerPush.registerListener(new IMQConsumerListener() {
            @Override
            public ConsumerStatus consumer(MQMessage mqMessage, IMQConsumerListenerContext context) {
                System.out.println("---------- 自定义 " + JSON.toJSONString(mqMessage));
                return ConsumerStatus.SUCCESS;
            }
        });
    }
}
