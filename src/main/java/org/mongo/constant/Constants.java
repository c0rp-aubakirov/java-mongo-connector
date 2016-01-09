package org.mongo.constant;

/**
 * User: Sanzhar Aubakirov
 * Date: 1/9/16
 */
public class Constants {
    // cluster.name of ElasticSearch. You can find it in elasticsearch.yml
    public static final String ELASTIC_APPLICATION_NAME = "my-application";

    public static final String ELASTIC_HOST = "localhost";
    public static final int ELASTIC_PORT = 9300;


    public static final String QUEUE_NAME = "oplog_queue";

    // ElasticSearch index and mapping constants
    public static final String INDEX_NAME = "emer"; // DB name in Mongo  == index name in Elastic
    public static final String[] supportedMappings =  {"Message"}; // Collection in Mongo == mapping in Elastic
}
