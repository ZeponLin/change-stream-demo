package com.example.demo.web;

import com.example.demo.model.CallbackChangeStream;
import com.example.demo.model.User;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest;
import org.springframework.data.mongodb.core.messaging.DefaultMessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.MessageListener;
import org.springframework.data.mongodb.core.messaging.MessageListenerContainer;
import org.springframework.data.mongodb.core.messaging.Subscription;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Objects;

@Slf4j
@Component
public class ChangeStreamListener {

    private final MessageListenerContainer listenerContainer;

    private final String databaseName;

    public ChangeStreamListener(MongoTemplate template) {
        this.listenerContainer = new DefaultMessageListenerContainer(template);
        this.databaseName = template.getDb().getName();
        this.init();
    }

    private void init() {
        listenerContainer.start();
    }

    @PreDestroy
    private void destroy() {
        if (listenerContainer != null && listenerContainer.isRunning()) {
            this.listenerContainer.stop();
        }
    }

    public void registerLister() {
        registerListener("user", userListener(), User.class);
    }

    private MessageListener<ChangeStreamDocument<Document>, User> userListener() {
        return createMessageListener(new CallbackChangeStream<User>() {
            @Override
            public void insert(ChangeStreamDocument<Document> raw, User body) {
                log.info("new user is inserted, content is {}.", body.toString());
            }

            @Override
            public void update(ChangeStreamDocument<Document> raw, User body) {
                log.info("user is updated, content is {}.", body);
            }

            @Override
            public void delete(ChangeStreamDocument<Document> raw, User body) {
                log.info("user of {} is deleted!", raw.getDocumentKey().getObjectId("_id").getValue().toHexString());
            }
        });
    }

    private <T> void registerListener(String collectionName, MessageListener<ChangeStreamDocument<Document>, T> listener, Class<T> bodyType) {
        ChangeStreamOptions changeStreamOptions = ChangeStreamOptions.builder().returnFullDocumentOnUpdate().build();
        registerListener(collectionName, listener, bodyType, changeStreamOptions);
    }

    private <T> void registerListener(String collectionName, MessageListener<ChangeStreamDocument<Document>, T> listener, Class<T> bodyType, ChangeStreamOptions changeStreamOptions) {
        ChangeStreamRequest.ChangeStreamRequestOptions options = new ChangeStreamRequest.ChangeStreamRequestOptions(this.databaseName, collectionName, changeStreamOptions);
        Subscription subscription = listenerContainer.register(new ChangeStreamRequest<>(listener, options), bodyType);
        try {
            if (subscription.await(Duration.ofSeconds(10))) {
                log.info("collection:{} listener register success! ", collectionName);
            } else {
                log.info("collection:{} listener register fail or time out! ", collectionName);
            }
        } catch (InterruptedException e) {
            log.error("interrupted! ", e);
            Thread.currentThread().interrupt();
        }
    }

    private <T> MessageListener<ChangeStreamDocument<Document>, T> createMessageListener(CallbackChangeStream<T> callback) {
        return listenMsg -> {
            ChangeStreamDocument<Document> raw = listenMsg.getRaw();
            OperationType operationType = Objects.requireNonNull(raw).getOperationType();
            switch (operationType) {
                case INSERT:
                    callback.insert(raw, listenMsg.getBody());
                    break;
                case REPLACE:
                case UPDATE:
                    callback.update(raw, listenMsg.getBody());
                    break;
                case DELETE:
                    callback.delete(raw, listenMsg.getBody());
                    break;
                default:
                    break;
            }
        };
    }
}
