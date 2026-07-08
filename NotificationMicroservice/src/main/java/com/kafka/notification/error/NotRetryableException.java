package com.kafka.notification.error;

public class NotRetryableException extends RuntimeException{
    public NotRetryableException(Throwable cause) {
        super(cause);
    }

    public NotRetryableException(String message) {
        super(message);
    }
    public NotRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
