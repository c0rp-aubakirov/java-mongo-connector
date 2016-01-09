package org.mongo.tail;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.UpdateOptions;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.mongo.tail.exception.QueuePublishException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

// Code based on this tutorial http://jaihirsch.github.io/straw-in-a-haystack/mongodb/2014/08/18/mongo-oplog-tailing/
public class OplogTail implements Runnable {
    private static final String OPLOG_FILENAME = "oplog_timestamp";
    private final Gson gson = new GsonBuilder().create();
    private final Logger logger = Logger.getLogger("OPLOG");

    private MongoClient client = null;
    private BSONTimestamp lastTimeStamp = null;
    private final List<ITailType> tailers;

    public OplogTail(Map.Entry<String, MongoClient> client, List<ITailType> tailers) throws IOException {
        this.tailers = tailers;
        this.client = client.getValue();
        this.lastTimeStamp = readTimestamp();
    }

    @Override
    public void run() {
        final MongoCollection<Document> fromCollection = client.getDatabase("local").getCollection("oplog.rs");
        final BasicDBObject timeQuery = getTimeQuery();
        logger.info("\n\tStart tailing with time query:\t" + timeQuery);
        final MongoCursor<Document> opCursor = fromCollection.find(timeQuery)
                .sort(new BasicDBObject("$natural", 1))
                .cursorType(CursorType.TailableAwait)
                .noCursorTimeout(true).iterator();
        try {
            while (true) {
                // We need this strange if, because oplog can be empty but it is not closed
                // We only have to wait until next CRUD appear
                if (opCursor.hasNext()) {
                    final Document nextOp = opCursor.next();
                    final BsonTimestamp ts = (BsonTimestamp) nextOp.get("ts");
                    lastTimeStamp = new BSONTimestamp(ts.getTime(), ts.getInc());
                    final UpdateOptions updateOptions = new UpdateOptions();
                    updateOptions.upsert(true);
                    for (ITailType tailer : tailers) {
                        tailer.tailOp(nextOp);
                        persistTimeStamp(lastTimeStamp);
                    }
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "\n\tFailed to write timestamp to file\n", e);
        } catch (QueuePublishException e) {
            logger.log(Level.SEVERE, "\n\tSomething happened to the OpenMQ\n", e);
        } finally {
            for (ITailType tailer : tailers) {
                tailer.close();
            }
            logger.info("Tailers closed");
        }
    }

    /**
     * Return time query depending on lastTimeStamp
     * If lastTimeStamp is null, will return query that fetch ALL data from oplog
     * if lastTimeStamp is not null, return query that fetch data started from this time stamp
     */
    private BasicDBObject getTimeQuery() {
        final BasicDBObject timeQuery = new BasicDBObject();
        if (lastTimeStamp != null) {
            timeQuery.put("ts", BasicDBObjectBuilder.start("$gt", lastTimeStamp).get());
        }
        return timeQuery;
    }

    private void persistTimeStamp(BSONTimestamp timestamp) throws IOException {
        try (final Writer writer = new FileWriterWithEncoding(OPLOG_FILENAME, Charsets.UTF_8)) {
            gson.toJson(timestamp, writer);
            writer.flush();
        }
    }

    private BSONTimestamp readTimestamp() throws IOException {
        final BSONTimestamp noPreviousTimestamp = null;
        final File file = new File(OPLOG_FILENAME);
        if (file.exists()) {
            try (FileReader fileReader = new FileReader(file)) {
                final JsonReader reader = new JsonReader(fileReader);
                final BSONTimestamp lastTimeStamp = gson.fromJson(reader, BSONTimestamp.class);
                return lastTimeStamp != null ? lastTimeStamp : noPreviousTimestamp;
            }
        }
        return noPreviousTimestamp;
    }

}
