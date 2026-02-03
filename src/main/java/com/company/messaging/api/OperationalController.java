package com.company.messaging.api;

import com.company.messaging.core.audit.AuditService;
import com.company.messaging.core.diagnostics.DiagnosticService;
import com.company.messaging.core.tracing.DistributedTracer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api/ops")
@RequiredArgsConstructor
public class OperationalController {
    
    private final DiagnosticService diagnosticService;
    private final AuditService auditService;
    private final DistributedTracer tracer;
    
    @Value("${audit.directory:./audit}")
    private String auditDirectory;
    
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        return ResponseEntity.ok(diagnosticService.getDiagnosticInfo());
    }
    
    @GetMapping("/audit/search")
    public Flux<AuditService.AuditEvent> searchAudit(
            @RequestParam(required = false) String traceId,
            @RequestParam(required = false) String correlationId,
            @RequestParam(required = false) String payloadId,
            @RequestParam(required = false) String businessKey,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @RequestParam(defaultValue = "100") int limit) {
        
        return auditService.getAuditStream()
            .filter(event -> matchesCriteria(event, traceId, correlationId, payloadId, businessKey, date))
            .take(limit);
    }
    
    @GetMapping("/audit/download")
    public ResponseEntity<Resource> downloadAudit(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date,
            @RequestParam(required = false) String hour) throws IOException {
        
        String dateStr = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        Path auditPath = Paths.get(auditDirectory, dateStr);
        
        if (!Files.exists(auditPath)) {
            return ResponseEntity.notFound().build();
        }
        
        String filename;
        if (hour != null) {
            filename = String.format("audit_%s_%s.json", 
                dateStr.replace("-", ""), hour);
        } else {
            // Create a zip of all files for the day
            filename = String.format("audit_%s.zip", dateStr.replace("-", ""));
            // Implementation for creating zip would go here
        }
        
        Path filePath = auditPath.resolve(filename);
        if (!Files.exists(filePath)) {
            return ResponseEntity.notFound().build();
        }
        
        return ResponseEntity.ok()
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .header(HttpHeaders.CONTENT_DISPOSITION, 
                   "attachment; filename=\"" + filename + "\"")
            .body(new FileSystemResource(filePath));
    }
    
    @GetMapping("/errors/recent")
    public ResponseEntity<List<Map<String, Object>>> getRecentErrors(
            @RequestParam(defaultValue = "50") int limit) {
        
        List<Map<String, Object>> errors = diagnosticService.getDiagnosticInfo()
            .entrySet().stream()
            .filter(e -> e.getKey().contains("error"))
            .limit(limit)
            .collect(Collectors.toList());
        
        return ResponseEntity.ok(errors);
    }
    
    @PostMapping("/trace/{traceId}")
    public ResponseEntity<Map<String, Object>> getTraceDetails(@PathVariable String traceId) {
        Map<String, Object> traceInfo = Map.of(
            "traceId", traceId,
            "status", "Collected",
            "timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        );
        
        // In a real implementation, query trace storage (Jaeger, Zipkin, etc.)
        return ResponseEntity.ok(traceInfo);
    }
    
    @PostMapping("/payload/{payloadId}/replay")
    public Mono<ResponseEntity<Map<String, Object>>> replayPayload(@PathVariable String payloadId) {
        return Mono.fromCallable(() -> {
            log.info("Replay requested for payload: {}", payloadId);
            
            // Implementation would retrieve payload from storage and reprocess
            return ResponseEntity.ok(Map.of(
                "payloadId", payloadId,
                "status", "Replay scheduled",
                "message", "Payload will be reprocessed"
            ));
        });
    }
    
    @PostMapping("/protocol/{type}/restart")
    public ResponseEntity<Map<String, Object>> restartProtocol(
            @PathVariable String type,
            @RequestParam String direction) {
        
        log.info("Restart requested for {} protocol: {}", direction, type);
        
        // Implementation would restart the specific protocol
        return ResponseEntity.ok(Map.of(
            "protocol", type,
            "direction", direction,
            "status", "Restart initiated",
            "timestamp", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        ));
    }
    
    private boolean matchesCriteria(AuditService.AuditEvent event, 
                                   String traceId, String correlationId, 
                                   String payloadId, String businessKey, 
                                   LocalDate date) {
        if (traceId != null && !traceId.equals(event.getTraceId())) return false;
        if (correlationId != null && !correlationId.equals(event.getCorrelationId())) return false;
        if (payloadId != null && !payloadId.equals(event.getPayloadId())) return false;
        if (businessKey != null && !businessKey.equals(event.getBusinessKey())) return false;
        if (date != null && !event.getTimestamp().atZone(java.time.ZoneId.systemDefault())
            .toLocalDate().equals(date)) return false;
        return true;
    }
}
