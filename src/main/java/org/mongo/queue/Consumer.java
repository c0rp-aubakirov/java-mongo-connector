package org.mongo.queue;

/**
 * User: Sanzhar Aubakirov
 * Date: 1/9/16
 */

import org.bson.Document;
import org.bson.types.ObjectId;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.mongo.constant.Constants;

import javax.jms.*;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.logging.Logger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class Consumer extends AbstractConsumer {
    protected final Logger log = Logger.getLogger("OMQ_CONSUMER");
    protected final ConnectionFactory cf;
    private final Connection connection;
    private final Session session;
    private final Destination destination;
    private final MessageConsumer consumer;

    public Consumer() throws JMSException, UnknownHostException {
        super();
        /**
         * Default port and host for ConnectionFactory:
         * imqBrokerHostName=localhost
         * imqBrokerHostPort=7676
         */
        cf = new com.sun.messaging.ConnectionFactory();
        connection = cf.createConnection();
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        destination = session.createQueue(Constants.QUEUE_NAME);
        consumer = session.createConsumer(destination);
    }

    public void consume() throws JMSException {
        final MessageListener messageListener = message -> {
            final Document document;
            try {
                document = Document.parse(((TextMessage) message).getText());
                final String operation = document.getString("op"); // get type of CRUD operation
                final Document o = (Document) document.get("o"); // this is where object lives
                final String ns = (String) document.get(
                        "ns"); // this is index.mapping information. usually looks like "index.mapping"
                final String index = ns.split("\\.")[0];
                final String mapping = ns.split("\\.")[1];

                // Check if we are supporting such mapping. There is too much information in MongoDB oplog
                if (!Arrays.asList(Constants.supportedMappings).contains(mapping)) {
                    log.info("\n\tWe are not supporting other mappings than supportedMappings" +
                                     "\nRequested mapping name is:\t" + mapping);
                    session.commit();  // remove message from queue
                    return;
                }

                final ObjectId id = o.getObjectId("_id");
                final Object className = getString(o, "className");
                final Object date = getDate(o, "date");
                final Object title = getString(o, "title");
                final Object type = getString(o, "type");
                final Object body = getString(o, "body");
                final Object tags = getListOfStrings(o, "tags");

                final XContentBuilder builder = jsonBuilder()
                        .startObject()
                        .field("date", date)
                        .field("title", title)
                        .field("tags", tags)
                        .field("body", body);

                final boolean result = doAction(operation, index, mapping, id, builder);
                if (result) {
                    session.commit();
                } else {
                    session.rollback();
                }
            } catch (JMSException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        consumer.setMessageListener(messageListener);
        connection.start();
    }

    public void closeConnection() {
        try {
            consumer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
