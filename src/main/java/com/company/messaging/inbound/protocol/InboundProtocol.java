package com.company.messaging.inbound.protocol;

import com.company.messaging.core.model.Payload;
import reactor.core.publisher.Flux;

public interface InboundProtocol {
    String getType();
    void initialize(Map<String, Object> config);
    Flux<Payload> receive();
    void shutdown();
    boolean isRunning();
}
