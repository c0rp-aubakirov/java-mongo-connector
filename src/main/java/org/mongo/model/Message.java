package org.mongo.model;

import java.util.Date;
import java.util.List;

/**
 * User: Sanzhar Aubakirov
 * Date: 1/9/16
 */
public class Message {
    private final String title;
    private final String body;
    private final List<String> tags;
    private final Date date;

    public Message(String body, String title, List<String> tags, Date date) {
        this.body = body;
        this.title = title;
        this.tags = tags;
        this.date = date;
    }

    public String getBody() {
        return body;
    }

    public Date getDate() {
        return date;
    }

    public List<String> getTags() {
        return tags;
    }

    public String getTitle() {
        return title;
    }
}
