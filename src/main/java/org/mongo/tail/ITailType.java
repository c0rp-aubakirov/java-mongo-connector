package org.mongo.tail;

import org.bson.Document;
import org.mongo.tail.exception.QueuePublishException;

import java.io.IOException;

/**
 * Basic interface for tailers
 */
public interface ITailType {
    void tailOp(Document op) throws IOException, QueuePublishException;
    void close();
}
