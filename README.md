# java-mongo-connector

This project is Java based alternative to mongo-connector. It is reading MongoDB oplog and reproduce appropriate actions in ElasticSearch. Code based on [this](http://jaihirsch.github.io/straw-in-a-haystack/mongodb/2014/08/18/mongo-oplog-tailing/) tutorial


**Producer** is tailing MongoDB oplog and sends messages to [JMS queue](https://mq.java.net/)

**Consumer** is read messages from queue and do either INSERT, DELETE or UPDATE

# Prepare soft

1. install and run OpenMQ with default settings.
2. install and run MongoDB with default settings. And have collection and data inside.
3. install ElasticSearch with default settings.


# Run examples

There are MainProducer.class and MainConsumer.class for testing purposes.
