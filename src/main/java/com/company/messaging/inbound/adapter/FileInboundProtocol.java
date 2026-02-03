package com.company.messaging.inbound.adapter;

import com.company.messaging.inbound.protocol.InboundProtocol;
import com.company.messaging.core.model.Payload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.file.*;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
@ConditionalOnProperty(name = "inbound.file.enabled", havingValue = "true")
public class FileInboundProtocol implements InboundProtocol {
    
    @Value("${inbound.file.directory:./inbound}")
    private String directory;
    
    private WatchService watchService;
    private volatile boolean running = false;
    
    @Override
    public String getType() {
        return "file";
    }
    
    @Override
    public void initialize(Map<String, Object> config) {
        if (config.containsKey("directory")) {
            directory = (String) config.get("directory");
        }
        
        try {
            Path path = Paths.get(directory);
            if (!Files.exists(path)) {
                Files.createDirectories(path);
            }
            watchService = FileSystems.getDefault().newWatchService();
            path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
            running = true;
            log.info("File inbound protocol initialized for directory: {}", directory);
        } catch (IOException e) {
            log.error("Failed to initialize file inbound protocol", e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public Flux<Payload> receive() {
        return Flux.create(sink -> {
            Thread thread = new Thread(() -> {
                while (running) {
                    try {
                        WatchKey key = watchService.take();
                        for (WatchEvent<?> event : key.pollEvents()) {
                            Path filename = (Path) event.context();
                            Path filePath = Paths.get(directory).resolve(filename);
                            
                            Payload payload = new Payload();
                            payload.setId(UUID.randomUUID().toString());
                            payload.setData(Files.readAllBytes(filePath));
                            payload.setTimestamp(Instant.now());
                            payload.setSourceProtocol("file");
                            payload.setStatus(PayloadStatus.RECEIVED);
                            
                            sink.next(payload);
                            Files.delete(filePath); // Optional: remove after processing
                        }
                        key.reset();
                    } catch (Exception e) {
                        log.error("Error in file watch service", e);
                        sink.error(e);
                    }
                }
            });
            thread.setDaemon(true);
            thread.start();
            
            sink.onDispose(() -> running = false);
        }).subscribeOn(Schedulers.boundedElastic());
    }
    
    @Override
    public void shutdown() {
        running = false;
        try {
            if (watchService != null) {
                watchService.close();
            }
        } catch (IOException e) {
            log.error("Error shutting down file protocol", e);
        }
    }
    
    @Override
    public boolean isRunning() {
        return running;
    }
}
