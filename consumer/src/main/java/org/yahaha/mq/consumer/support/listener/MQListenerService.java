package org.yahaha.mq.consumer.support.listener;

import com.alibaba.fastjson.JSON;
import com.github.houbb.heaven.annotation.NotThreadSafe;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import lombok.extern.slf4j.Slf4j;
import org.yahaha.mq.common.constant.MessageStatusConst;
import org.yahaha.mq.common.dto.req.component.MQMessage;
import org.yahaha.mq.common.resp.ConsumerStatus;
import org.yahaha.mq.consumer.api.IMQConsumerListener;
import org.yahaha.mq.consumer.api.IMQConsumerListenerContext;

import java.util.concurrent.TimeUnit;

@NotThreadSafe
@Slf4j
public class MQListenerService implements IMQListenerService {

    private IMQConsumerListener mqConsumerListener;
    private Retryer<ConsumerStatus> retryer = RetryerBuilder.<ConsumerStatus>newBuilder()
            .retryIfExceptionOfType(Exception.class) // 发生异常时重试
            .retryIfResult(result -> result.getCode() != MessageStatusConst.CONSUMER_SUCCESS) // 如果结果为false则重试
            .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS)) // 固定间隔1秒
            .withStopStrategy(StopStrategies.stopAfterAttempt(9)) // 最大重试次数为3次
            .build();

    @Override
    public void register(IMQConsumerListener listener) {
        this.mqConsumerListener = listener;
    }

    @Override
    public ConsumerStatus consumer(MQMessage mqMessage, IMQConsumerListenerContext context) {
        if(mqConsumerListener == null) {
            log.warn("当前监听类为空，直接忽略处理。message: {}", JSON.toJSON(mqMessage));
            return ConsumerStatus.SUCCESS;
        } else {
            ConsumerStatus result = ConsumerStatus.FAILED;
            try {
                result  = retryer.call(()-> {
                    return mqConsumerListener.consumer(mqMessage, context);
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
            log.debug("消费出错，messageID: {}, result: {}", mqMessage.getTraceId(), result.getCode());
            return result;
        }
    }
}
