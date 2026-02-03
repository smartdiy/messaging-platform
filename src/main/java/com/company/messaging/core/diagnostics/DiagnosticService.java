package com.company.messaging.core.diagnostics;

import com.company.messaging.inbound.protocol.InboundProtocol;
import com.company.messaging.outbound.protocol.OutboundProtocol;
import com.company.messaging.plugin.registry.ProtocolRegistry;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.lang.management.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class DiagnosticService implements HealthIndicator {
    
    @Autowired
    private ProtocolRegistry protocolRegistry;
    
    private final Map<String, ComponentHealth> componentHealth = new ConcurrentHashMap<>();
    private final Map<String, QueueMetrics> queueMetrics = new ConcurrentHashMap<>();
    private final Map<String, ErrorStats> errorStats = new ConcurrentHashMap<>();
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final Instant startupTime = Instant.now();
    
    @Data
    public static class ComponentHealth {
        private String name;
        private String type;
        private String status;
        private String lastCheck;
        private String errorMessage;
        private Map<String, Object> details;
        private Instant lastChange;
        
        public ComponentHealth(String name, String type) {
            this.name = name;
            this.type = type;
            this.status = "UNKNOWN";
            this.lastCheck = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            this.details = new HashMap<>();
        }
    }
    
    @Data
    public static class QueueMetrics {
        private String queueName;
        private long currentSize;
        private long maxSize;
        private long totalProcessed;
        private long processingRate;
        private Instant lastProcessed;
        private Map<String, Long> waitingTimePercentiles;
    }
    
    @Data
    public static class ErrorStats {
        private String errorCode;
        private long count;
        private Instant firstOccurrence;
        private Instant lastOccurrence;
        private List<String> recentPayloads;
    }
    
    @Scheduled(fixedRate = 30000) // Every 30 seconds
    public void collectSystemMetrics() {
        try {
            // Collect JVM metrics
            Runtime runtime = Runtime.getRuntime();
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
            
            ComponentHealth jvmHealth = new ComponentHealth("jvm", "system");
            jvmHealth.setStatus("HEALTHY");
            jvmHealth.getDetails().put("memory.used", runtime.totalMemory() - runtime.freeMemory());
            jvmHealth.getDetails().put("memory.total", runtime.totalMemory());
            jvmHealth.getDetails().put("memory.max", runtime.maxMemory());
            jvmHealth.getDetails().put("threads.active", threadBean.getThreadCount());
            jvmHealth.getDetails().put("threads.peak", threadBean.getPeakThreadCount());
            jvmHealth.getDetails().put("uptime", ManagementFactory.getRuntimeMXBean().getUptime());
            
            componentHealth.put("jvm", jvmHealth);
            
            // Check protocol health
            checkProtocolHealth();
            
        } catch (Exception e) {
            log.error("Error collecting system metrics", e);
        }
    }
    
    private void checkProtocolHealth() {
        protocolRegistry.getAvailableInboundProtocols().forEach(protocolType -> {
            InboundProtocol protocol = protocolRegistry.getInboundProtocol(protocolType);
            if (protocol != null) {
                ComponentHealth health = new ComponentHealth(protocolType, "inbound");
                health.setStatus(protocol.isRunning() ? "HEALTHY" : "UNHEALTHY");
                health.getDetails().put("running", protocol.isRunning());
                componentHealth.put("inbound." + protocolType, health);
            }
        });
        
        protocolRegistry.getAvailableOutboundProtocols().forEach(protocolType -> {
            OutboundProtocol protocol = protocolRegistry.getOutboundProtocol(protocolType);
            if (protocol != null) {
                ComponentHealth health = new ComponentHealth(protocolType, "outbound");
                health.setStatus(protocol.isRunning() ? "HEALTHY" : "UNHEALTHY");
                health.getDetails().put("running", protocol.isRunning());
                componentHealth.put("outbound." + protocolType, health);
            }
        });
    }
    
    public void recordProcessing(String payloadId, String protocol, long durationMs, boolean success) {
        totalProcessed.incrementAndGet();
        if (!success) {
            totalErrors.incrementAndGet();
        }
        
        String key = protocol + ".processing";
        queueMetrics.computeIfAbsent(key, k -> {
            QueueMetrics metrics = new QueueMetrics();
            metrics.setQueueName(k);
            return metrics;
        });
    }
    
    public void recordError(String errorCode, String payloadId, String component) {
        errorStats.computeIfAbsent(errorCode, k -> {
            ErrorStats stats = new ErrorStats();
            stats.setErrorCode(k);
            stats.setCount(0);
            stats.setFirstOccurrence(Instant.now());
            stats.setRecentPayloads(new ArrayList<>());
            return stats;
        });
        
        ErrorStats stats = errorStats.get(errorCode);
        stats.setCount(stats.getCount() + 1);
        stats.setLastOccurrence(Instant.now());
        if (stats.getRecentPayloads().size() < 10) {
            stats.getRecentPayloads().add(payloadId);
        }
    }
    
    public Map<String, Object> getDiagnosticInfo() {
        Map<String, Object> info = new HashMap<>();
        
        info.put("uptime", ManagementFactory.getRuntimeMXBean().getUptime());
        info.put("startupTime", startupTime);
        info.put("totalProcessed", totalProcessed.get());
        info.put("totalErrors", totalErrors.get());
        info.put("errorRate", totalProcessed.get() > 0 ? 
            (double) totalErrors.get() / totalProcessed.get() : 0);
        info.put("componentHealth", componentHealth);
        info.put("errorStats", errorStats);
        info.put("queueMetrics", queueMetrics);
        info.put("systemLoad", getSystemLoad());
        
        return info;
    }
    
    private Map<String, Object> getSystemLoad() {
        Map<String, Object> load = new HashMap<>();
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            com.sun.management.OperatingSystemMXBean sunOsBean = 
                (com.sun.management.OperatingSystemMXBean) osBean;
            load.put("cpuLoad", sunOsBean.getCpuLoad());
            load.put("systemCpuLoad", sunOsBean.getSystemCpuLoad());
            load.put("freeMemory", sunOsBean.getFreeMemorySize());
            load.put("totalMemory", sunOsBean.getTotalMemorySize());
        }
        
        return load;
    }
    
    @Override
    public Health health() {
        Map<String, Object> details = getDiagnosticInfo();
        
        boolean isHealthy = componentHealth.values().stream()
            .allMatch(ch -> "HEALTHY".equals(ch.getStatus()));
        
        if (isHealthy) {
            return Health.up().withDetails(details).build();
        } else {
            return Health.down().withDetails(details).build();
        }
    }
}
