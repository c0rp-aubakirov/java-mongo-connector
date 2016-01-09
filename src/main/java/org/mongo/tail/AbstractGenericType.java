package org.mongo.tail;

/**
 * User: Sanzhar Aubakirov
 * Date: 1/7/16
 */
import org.bson.Document;
import org.mongo.tail.exception.QueuePublishException;

import javax.jms.JMSException;
import java.io.IOException;

public abstract class AbstractGenericType implements ITailType {
    @Override
    public void tailOp(Document op) throws QueuePublishException {
        try{
            switch ((String) op.get("op")) { // usually op looks like {"op": "i"} or {"op": "u"}
                case "u": // update event in mongodb
                    if ("repl.time".equals(op.getString("ns"))) {}
                    else handle(op);
                    break;
                case "i": handle(op); // insert event in mongodb
                    break;
                case "d": handle(op); // delete event in mongodb
                    break;
                default: handleOtherOps(op);
                    break;
            }
        }catch (IOException | JMSException e){
            throw new QueuePublishException("Failed to publish message to queue");
        }
    }
    protected void handleOtherOps(Document op) {
        System.out.println("Non-handled operation: " + op);
    }
    protected abstract void handle(Document op) throws IOException, JMSException;
    public void close() {}
}
