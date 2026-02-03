package com.company.messaging.core.tracing;

import lombok.Data;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Data
public class TraceContext {
    private String traceId;
    private String spanId;
    private String parentSpanId;
    private String correlationId;
    private Instant startTime;
    private Map<String, String> tags;
    private String serviceName;
    private String operation;
    
    public TraceContext() {
        this.traceId = UUID.randomUUID().toString();
        this.spanId = UUID.randomUUID().toString();
        this.startTime = Instant.now();
        this.tags = new HashMap<>();
    }
    
    public TraceContext(String traceId, String spanId) {
        this.traceId = traceId;
        this.spanId = spanId;
        this.startTime = Instant.now();
        this.tags = new HashMap<>();
    }
    
    public void addTag(String key, String value) {
        tags.put(key, value);
    }
    
    public String getTraceId() {
        return traceId != null ? traceId : "NO_TRACE";
    }
}
