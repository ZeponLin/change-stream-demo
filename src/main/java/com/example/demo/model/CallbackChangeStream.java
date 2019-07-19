package com.example.demo.model;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

public interface CallbackChangeStream<T> {

    void insert(ChangeStreamDocument<Document> raw, T body);

    default void update(ChangeStreamDocument<Document> raw, T body) {
    }

    default void delete(ChangeStreamDocument<Document> raw, T body) {
    }
}
