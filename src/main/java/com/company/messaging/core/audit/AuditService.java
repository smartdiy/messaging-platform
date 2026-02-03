package com.company.messaging.core.audit;

import com.company.messaging.core.model.EnhancedPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
public class AuditService {
    
    @Value("${audit.enabled:true}")
    private boolean enabled;
    
    @Value("${audit.directory:./audit}")
    private String auditDirectory;
    
    @Value("${audit.retention-days:30}")
    private int retentionDays;
    
    private final Sinks.Many<AuditEvent> sink = Sinks.many().multicast().onBackpressureBuffer(10000);
    private final ConcurrentLinkedQueue<AuditEvent> buffer = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean writing = new AtomicBoolean(false);
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final DateTimeFormatter DATE_FORMATTER = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FILE_FORMATTER = 
        DateTimeFormatter.ofPattern("yyyyMMdd_HH");
    
    public void auditEvent(AuditEvent event) {
        if (enabled) {
            sink.emitNext(event, Sinks.EmitFailureHandler.FAIL_FAST);
            buffer.offer(event);
        }
    }
    
    public void auditPayload(String action, EnhancedPayload payload, String status, String details) {
        AuditEvent event = AuditEvent.builder()
            .timestamp(Instant.now())
            .action(action)
            .traceId(payload.getTraceContext().getTraceId())
            .correlationId(payload.getTraceContext().getCorrelationId())
            .payloadId(payload.getId())
            .sourceProtocol(payload.getSourceProtocol())
            .targetProtocol(payload.getTargetProtocol())
            .status(status)
            .details(details)
            .businessKey(payload.getBusinessKey())
            .errorCode(payload.getErrorCode())
            .retryCount(payload.getRetryCount())
            .metadata(payload.getMetadata())
            .build();
        
        auditEvent(event);
    }
    
    @Scheduled(fixedDelay = 5000) // Write every 5 seconds
    public void flushToDisk() {
        if (!enabled || writing.get() || buffer.isEmpty()) {
            return;
        }
        
        if (writing.compareAndSet(false, true)) {
            try {
                String dateStr = LocalDate.now().format(DATE_FORMATTER);
                String hourStr = Instant.now().atZone(java.time.ZoneId.systemDefault())
                    .format(FILE_FORMATTER);
                
                Path auditPath = Paths.get(auditDirectory, dateStr);
                Files.createDirectories(auditPath);
                
                String filename = String.format("audit_%s_%s.json", dateStr, hourStr);
                Path filePath = auditPath.resolve(filename);
                
                try (var writer = Files.newBufferedWriter(filePath, 
                        StandardOpenOption.CREATE, 
                        StandardOpenOption.APPEND)) {
                    
                    AuditEvent event;
                    while ((event = buffer.poll()) != null) {
                        writer.write(objectMapper.writeValueAsString(event));
                        writer.newLine();
                    }
                }
                
            } catch (IOException e) {
                log.error("Failed to write audit log", e);
            } finally {
                writing.set(false);
            }
        }
    }
    
    @Scheduled(cron = "0 0 2 * * *") // Daily at 2 AM
    public void cleanupOldAudits() {
        if (!enabled) return;
        
        try {
            Path auditBase = Paths.get(auditDirectory);
            if (!Files.exists(auditBase)) return;
            
            Instant cutoff = Instant.now().minusSeconds(retentionDays * 24 * 60 * 60);
            
            Files.list(auditBase)
                .filter(Files::isDirectory)
                .filter(path -> {
                    try {
                        LocalDate dirDate = LocalDate.parse(path.getFileName().toString(), DATE_FORMATTER);
                        return dirDate.atStartOfDay(java.time.ZoneId.systemDefault())
                            .toInstant().isBefore(cutoff);
                    } catch (Exception e) {
                        return false;
                    }
                })
                .forEach(path -> {
                    try {
                        Files.walk(path)
                            .sorted((a, b) -> -a.compareTo(b))
                            .forEach(p -> {
                                try { Files.delete(p); } catch (IOException ignored) {}
                            });
                        log.info("Cleaned up old audit directory: {}", path);
                    } catch (IOException e) {
                        log.error("Failed to cleanup audit directory: {}", path, e);
                    }
                });
        } catch (IOException e) {
            log.error("Failed to cleanup old audits", e);
        }
    }
    
    public Flux<AuditEvent> getAuditStream() {
        return sink.asFlux();
    }
    
    @Data
    @Builder
    public static class AuditEvent {
        private Instant timestamp;
        private String action; // RECEIVE, PROCESS, SEND, ERROR, RETRY
        private String traceId;
        private String correlationId;
        private String payloadId;
        private String sourceProtocol;
        private String targetProtocol;
        private String status;
        private String details;
        private String businessKey;
        private String errorCode;
        private Integer retryCount;
        private Map<String, Object> metadata;
        private String component;
        private String hostname;
        
        public AuditEvent() {
            this.hostname = getHostname();
        }
        
        private String getHostname() {
            try {
                return java.net.InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                return "unknown";
            }
        }
    }
}
