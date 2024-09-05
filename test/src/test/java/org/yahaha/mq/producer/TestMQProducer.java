package org.yahaha.mq.producer;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.producer.dto.SendResult;

@Slf4j
public class TestMQProducer {
    public static void main(String[] args) {
        MQProducer mqProducer = new MQProducer();
//        mqProducer.appKey("test")
//                .appSecret("mq");
        mqProducer.start();

        long startTime = System.currentTimeMillis();
        for(int i = 0; i < 10000; i++) {
            MQMessage mqMessage = buildMessage(i);
            SendResult sendResult = mqProducer.send(mqMessage);
            System.out.println(JSON.toJSON(sendResult));
        }
        long endTime = System.currentTimeMillis();
        log.info("发送消息耗时：{}ms", endTime - startTime);
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
