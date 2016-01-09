package org.mongo.tail.exception;

/**
 * User: Sanzhar Aubakirov
 * Date: 1/8/16
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
