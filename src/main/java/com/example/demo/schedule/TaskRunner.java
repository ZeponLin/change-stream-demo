package com.example.demo.schedule;

import com.example.demo.web.ChangeStreamListener;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TaskRunner implements CommandLineRunner {

    private final ChangeStreamListener listener;

    @Override
    public void run(String... args) {
        log.info("register listeners start!");
        listener.registerLister();
        log.info("register listerners complete");
    }
}
