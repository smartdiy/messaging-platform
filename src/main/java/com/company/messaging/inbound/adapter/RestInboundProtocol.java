package com.company.messaging.inbound.adapter;

import com.company.messaging.inbound.protocol.InboundProtocol;
import com.company.messaging.core.model.Payload;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
@ConditionalOnProperty(name = "inbound.rest.enabled", havingValue = "true")
public class RestInboundProtocol implements InboundProtocol {
    
    private final Sinks.Many<Payload> sink = Sinks.many().multicast().onBackpressureBuffer();
    private boolean running = false;
    
    @Override
    public String getType() {
        return "rest";
    }
    
    @Override
    public void initialize(Map<String, Object> config) {
        running = true;
    }
    
    @Override
    public Flux<Payload> receive() {
        return sink.asFlux();
    }
    
    public Flux<ServerSentEvent<Payload>> handlePost(ServerRequest request) {
        return request.bodyToMono(Payload.class)
            .doOnNext(payload -> {
                payload.setId(UUID.randomUUID().toString());
                payload.setTimestamp(Instant.now());
                payload.setSourceProtocol("rest");
                payload.setStatus(PayloadStatus.RECEIVED);
                sink.emitNext(payload, Sinks.EmitFailureHandler.FAIL_FAST);
            })
            .map(payload -> ServerSentEvent.builder(payload).build());
    }
    
    @Override
    public void shutdown() {
        running = false;
        sink.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
    }
    
    @Override
    public boolean isRunning() {
        return running;
    }
}
