package org.mongo.queue;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.mongo.constant.Constants;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * User: Sanzhar Aubakirov
 * Date: 1/9/16
 */
public abstract class AbstractConsumer {
    protected final Logger log = Logger.getLogger("ABSTRACT_CONSUMER");
    public final Client client;

    public AbstractConsumer() throws UnknownHostException {
        final Settings settings = Settings.settingsBuilder().put("cluster.name", Constants.ELASTIC_APPLICATION_NAME).build();
        client = TransportClient.builder().settings(settings).build().addTransportAddress(
                new InetSocketTransportAddress(InetAddress.getByName(Constants.ELASTIC_HOST), Constants.ELASTIC_PORT));
    }

    public Boolean doAction(String operation, String index, String mapping, ObjectId id, XContentBuilder builder) {
        try {
            switch (operation) {
                case "i": // insert operation
                    client.prepareIndex(index, mapping, id.toString())
                            .setSource(builder)
                            .execute().actionGet();
                    return true;
                case "u": // update opertaion
                    client.prepareUpdate(index, mapping, id.toString())
                            .setDoc(builder).get();
                    return true;
                case "d": // delete operation
                    client.prepareDelete(index, mapping, id.toString()).get();
                    return true;
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, e.getMessage(), e);
        }
        return false;
    }

    /**
     * It should return not null because XContentBuilder will crash with NPE
     */
    public Object getListOfStrings(Document o, String fieldName) {
        final List<String> defaultValue = new ArrayList<>();
        try {
            return Optional.ofNullable(o.get(fieldName)).orElse(defaultValue);
        } catch (ClassCastException ignored) {
            log.info("\n\tCan't get List of String from field:\t" + fieldName
                             + "\n\t returning empty list");

            // Single value array can be stored as usual String
            return Optional.ofNullable(o.getString(fieldName)).orElse("");
        }
    }

    /**
     * Can be null
     */
    public Object getString(Document o, String fieldName) {
        final String defaultValue = "";
        try {
            return Optional.ofNullable(o.getString(fieldName)).orElse(defaultValue);
        } catch (ClassCastException ignored) {
            log.info("\n\tCan't get String from field:\t" + fieldName
                             + "\n\t returning null");
        }
        return defaultValue;
    }

    /**
     * Can be null
     */
    public Object getDate(Document o, String fieldName) {
        final Date defaultValue = new Date();
        try {
            return Optional.ofNullable(o.getDate(fieldName)).orElse(defaultValue);
        } catch (ClassCastException ignored) {
            log.info("\n\tCan't get Date from field:\t" + fieldName
                             + "\n\t returning todat date");
        }
        return defaultValue;
    }

    /**
     * Can be null
     */
    public Object getInteger(Document o, String fieldName) {
        final Integer defaultValue = 0;
        try {
            return Optional.ofNullable(o.getInteger(fieldName)).orElse(defaultValue);
        } catch (ClassCastException ignored) {
            log.info("\n\tCan't get Integer from field:\t" + fieldName
                             + "\n\t returning null");
        }
        return defaultValue;
    }
}
