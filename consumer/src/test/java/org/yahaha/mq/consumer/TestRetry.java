package org.yahaha.mq.consumer;


import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

@Slf4j
public class TestRetry {
    private static int cnt = 0;
    
    public static void main(String[] args) {
        Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
                .retryIfExceptionOfType(Exception.class) // 发生异常时重试
                .retryIfResult(result -> !result) // 如果结果为false则重试
                .withWaitStrategy(WaitStrategies.fixedWait(1, TimeUnit.SECONDS)) // 固定间隔1秒
                .withStopStrategy(StopStrategies.stopAfterAttempt(9)) // 最大重试次数为3次
                .build();

        try {
            retryer.call(
                    () -> {
                        return doSomething();
                    }
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static boolean doSomething() {
        try {
            cnt++;
            log.debug("do something, cnt: {}", cnt);
            return cnt % 5 == 0;
        } catch (Exception e) {
            log.error("do something error", e);
            return false;
        }
    }
}
