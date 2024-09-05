package org.yahaha.mq.consumer.dto;

import java.util.Objects;

public class MQTopicTagDto {
    /**
     * 标题名称
     */
    private String topicName;

    /**
     * 标签名称
     */
    private String tagRegex;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public String getTagRegex() {
        return tagRegex;
    }

    public void setTagRegex(String tagRegex) {
        this.tagRegex = tagRegex;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || this.getClass() != object.getClass()) return false;
        MQTopicTagDto dto = (MQTopicTagDto) object;
        return Objects.equals(topicName, dto.topicName) &&
                Objects.equals(tagRegex, dto.tagRegex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicName, tagRegex);
    }
}
