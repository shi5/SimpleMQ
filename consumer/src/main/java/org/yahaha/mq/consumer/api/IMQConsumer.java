package org.yahaha.mq.consumer.api;

public interface IMQConsumer {
    /**
     * 订阅
     * @param topicName topic 名称
     * @param tagRegex 标签正则
     */
    void subscribe(String topicName, String tagRegex);

    /**
     * 取消订阅
     * @param topicName topic 名称
     * @param tagRegex 标签正则
     */
    void unSubscribe(String topicName, String tagRegex);

    /**
     * 注册监听器
     * @param listener 监听器
     */
    void registerListener(final IMQConsumerListener listener);

}
