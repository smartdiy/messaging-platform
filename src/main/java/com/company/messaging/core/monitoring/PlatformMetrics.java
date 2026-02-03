package com.company.messaging.core.monitoring;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class PlatformMetrics {
    
    private final Counter inboundMessages;
    private final Counter outboundMessages;
    private final Counter processingErrors;
    private final Timer processingTimer;
    
    public PlatformMetrics(MeterRegistry registry) {
        inboundMessages = Counter.builder("messaging.inbound.total")
            .description("Total inbound messages")
            .register(registry);
        
        outboundMessages = Counter.builder("messaging.outbound.total")
            .description("Total outbound messages")
            .register(registry);
        
        processingErrors = Counter.builder("messaging.processing.errors")
            .description("Processing errors")
            .register(registry);
        
        processingTimer = Timer.builder("messaging.processing.time")
            .description("Message processing time")
            .register(registry);
    }
    
    public void recordInbound() {
        inboundMessages.increment();
    }
    
    public void recordOutbound() {
        outboundMessages.increment();
    }
    
    public void recordError() {
        processingErrors.increment();
    }
    
    public void recordProcessingTime(long time, TimeUnit unit) {
        processingTimer.record(time, unit);
    }
}
