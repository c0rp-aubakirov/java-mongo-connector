package org.mongo;

import org.mongo.queue.Consumer;

import javax.jms.JMSException;
import java.net.UnknownHostException;

/**
 * User: Sanzhar Aubakirov
 * Date: 1/9/16
 */
public class MainConsumer {

    public static void main(String[] args) {
        try {
            final Consumer consumer = new Consumer();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    consumer.closeConnection();
                }
            });

            consumer.consume();
        } catch (JMSException | UnknownHostException e) {
            e.printStackTrace();
        }
    }
}

