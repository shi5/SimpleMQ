package org.yahaha.mq.broker.persist.constant;

public enum AppendMessageStatus {
    PUT_OK,
    END_OF_FILE,
    MESSAGE_SIZE_EXCEEDED,
    UNKNOWN_ERROR,
}
