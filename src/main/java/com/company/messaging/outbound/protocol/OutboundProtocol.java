package com.company.messaging.outbound.protocol;

import com.company.messaging.core.model.Payload;
import reactor.core.publisher.Mono;

public interface OutboundProtocol {
    String getType();
    void initialize(Map<String, Object> config);
    Mono<Boolean> send(Payload payload);
    void shutdown();
    boolean isRunning();
}
