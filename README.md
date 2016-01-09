# java-mongo-connector

This project is Java based alternative to mongo-connector. It is reading MongoDB oplog and reproduce appropriate actions in ElasticSearch.


**Producer** is tailing MongoDB oplog and sends messages to [JMS queue](https://mq.java.net/)

**Consumer** is read messages from queue and do either INSERT, DELETE or UPDATE
