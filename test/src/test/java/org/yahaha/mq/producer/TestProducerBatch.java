package org.yahaha.mq.producer;

import com.alibaba.fastjson.JSON;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.producer.dto.SendBatchResult;

import java.util.ArrayList;
import java.util.List;

public class TestProducerBatch {
    public static void main(String[] args) {
        MQProducer mqProducer = new MQProducer();
        mqProducer.start();

        List<MQMessage> mqMessageList = new ArrayList<>();
        for(int i = 0; i < 20; i++) {
            MQMessage mqMessage = buildMessage(i);
            mqMessageList.add(mqMessage);
        }

        SendBatchResult sendResult = mqProducer.sendBatch(mqMessageList);
        System.out.println(JSON.toJSON(sendResult));
    }

    private static MQMessage buildMessage(int i) {
        String message = "HELLO MQ!" + i;
        MQMessage mqMessage = new MQMessage();
        mqMessage.setTopic("TOPIC");
        mqMessage.setTag("TAGA");
        mqMessage.setPayload(message);

        return mqMessage;
    }
}
