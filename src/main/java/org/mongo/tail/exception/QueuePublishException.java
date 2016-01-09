package org.mongo.tail.exception;

/**
 * This exception should be used if message producer failed to send message
 */
public class QueuePublishException extends Exception {
    public QueuePublishException(String message) {
        super(message);
    }

    public QueuePublishException(Throwable cause) {
        super(cause);
    }

    public QueuePublishException(String message, Throwable cause) {
        super(message, cause);
    }

}
