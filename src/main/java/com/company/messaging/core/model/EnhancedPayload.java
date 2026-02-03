package com.company.messaging.core.model;

import com.company.messaging.core.tracing.TraceContext;
import lombok.Data;
import lombok.EqualsAndHashCode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
public class EnhancedPayload extends Payload {
    private TraceContext traceContext;
    private List<ProcessingStep> processingSteps;
    private Map<String, Object> auditTrail;
    private String errorCode;
    private String errorDetails;
    private Instant retryTime;
    private int retryCount;
    private String businessKey;
    
    public EnhancedPayload() {
        this.processingSteps = new ArrayList<>();
        this.traceContext = new TraceContext();
    }
    
    public void addProcessingStep(String step, String status, String details) {
        ProcessingStep ps = new ProcessingStep();
        ps.setStep(step);
        ps.setStatus(status);
        ps.setDetails(details);
        ps.setTimestamp(Instant.now());
        ps.setTraceId(this.traceContext.getTraceId());
        ps.setSpanId(this.traceContext.getSpanId());
        this.processingSteps.add(ps);
    }
    
    @Data
    public static class ProcessingStep {
        private String step;
        private String status;
        private String details;
        private Instant timestamp;
        private String traceId;
        private String spanId;
        private String component;
        private Long durationMs;
    }
}
