package org.mongo;

import com.mongodb.MongoClient;
import javax.jms.JMSException;
import org.mongo.queue.Producer;
import org.mongo.tail.ITailType;
import org.mongo.tail.OplogTail;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * User: Sanzhar Aubakirov
 * Date: 1/9/16
 */
public class MainProducer {

    public static void main(String[] args) {
        try {
            final ITailType tailType = new Producer();
            final List<ITailType> tailers = new ArrayList<>();
            tailers.add(tailType);
            final MongoClient mongoClient = new MongoClient();
            final Map.Entry<String, MongoClient> client = new AbstractMap.SimpleEntry<>("local", mongoClient);
            final OplogTail oplogTail = new OplogTail(client, tailers);
            oplogTail.run();
        } catch (IOException | JMSException e) {
            e.printStackTrace();
        }
    }
}
