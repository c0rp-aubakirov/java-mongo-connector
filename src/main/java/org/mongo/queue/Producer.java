package org.mongo.queue;

/**
 * User: Sanzhar Aubakirov
 * Date: 1/9/16
 */

import org.bson.Document;
import org.mongo.constant.Constants;
import org.mongo.tail.AbstractGenericType;

import javax.jms.*;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Producer extends AbstractGenericType {
    protected final Logger log = Logger.getLogger("OMQ_PRODUCER");
    private final ConnectionFactory cf;
    private final Connection connection;
    private final Session session;
    private final Destination destination;
    private final MessageProducer producer;

    public Producer() throws JMSException {
        cf = new com.sun.messaging.ConnectionFactory();
        connection = cf.createConnection();
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        destination = session.createQueue(Constants.QUEUE_NAME);
        producer = session.createProducer(destination);
    }

    public boolean sendMessage(Document document) throws JMSException {
        try {
            // create message to send
            final TextMessage message = session.createTextMessage();
            message.setText(document.toJson());
            log.info("Sending message with title:\n\t" + document.getString("title"));
            producer.send(message);
            session.commit();
            return true;
        } catch (JMSException ex) {
            log.log(Level.SEVERE, ex.getMessage(), ex);
            session.rollback();
            return false;
        }
    }

    public void initConnection() throws JMSException {
        connection.start();
    }

    public void closeConnection() {
        // close everything
        try {
            producer.close();
            session.close();
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void handle(Document op) throws IOException, JMSException {
        sendMessage(op);
    }
}
