package com.company.messaging.core.processor;

import com.company.messaging.core.model.Payload;
import com.company.messaging.core.pool.InboundMessagePool;
import com.company.messaging.core.pool.OutboundMessagePool;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

@Slf4j
@Component
public class ProcessorEngine {
    
    @Autowired
    private InboundMessagePool inboundPool;
    
    @Autowired
    private OutboundMessagePool outboundPool;
    
    private final Map<String, Function<Payload, Payload>> processors = new ConcurrentHashMap<>();
    
    @PostConstruct
    public void init() {
        Flux<Payload> inboundStream = inboundPool.subscribe()
            .onBackpressureBuffer(1000)
            .publishOn(Schedulers.parallel())
            .doOnNext(this::processPayload);
        
        inboundStream.subscribe(
            payload -> log.debug("Processed payload: {}", payload.getId()),
            error -> log.error("Error in processing stream", error)
        );
    }
    
    @Async("taskExecutor")
    public void processPayload(Payload payload) {
        try {
            payload.setStatus(PayloadStatus.PROCESSING);
            log.info("Processing payload: {}", payload.getId());
            
            // Apply registered processors
            Payload processedPayload = payload;
            for (Function<Payload, Payload> processor : processors.values()) {
                processedPayload = processor.apply(processedPayload);
            }
            
            processedPayload.setStatus(PayloadStatus.PROCESSED);
            outboundPool.publish(processedPayload);
            
        } catch (Exception e) {
            log.error("Error processing payload: {}", payload.getId(), e);
            payload.setStatus(PayloadStatus.FAILED);
        }
    }
    
    public void registerProcessor(String name, Function<Payload, Payload> processor) {
        processors.put(name, processor);
        log.info("Registered processor: {}", name);
    }
    
    public void unregisterProcessor(String name) {
        processors.remove(name);
        log.info("Unregistered processor: {}", name);
    }
}
