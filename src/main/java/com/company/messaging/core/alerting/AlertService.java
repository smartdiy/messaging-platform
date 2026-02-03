package com.company.messaging.core.alerting;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Service
public class AlertService {
    
    @Value("${alerting.enabled:false}")
    private boolean enabled;
    
    @Value("${alerting.slack-webhook:}")
    private String slackWebhook;
    
    @Value("${alerting.teams-webhook:}")
    private String teamsWebhook;
    
    private final List<AlertRule> rules = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<String, Instant> lastAlertTime = new ConcurrentHashMap<>();
    private final WebClient webClient = WebClient.create();
    
    @Data
    public static class AlertRule {
        private String name;
        private String condition;
        private String severity; // INFO, WARNING, CRITICAL
        private int cooldownMinutes;
        private List<String> actions; // slack, teams, email, sms
    }
    
    @Data
    public static class Alert {
        private String id;
        private String ruleName;
        private String severity;
        private String message;
        private String component;
        private Instant timestamp;
        private Map<String, Object> metrics;
        private boolean acknowledged;
        private String acknowledgedBy;
        private Instant acknowledgedAt;
    }
    
    public void evaluateRules(Map<String, Object> metrics) {
        if (!enabled) return;
        
        rules.forEach(rule -> {
            try {
                if (evaluateCondition(rule.getCondition(), metrics)) {
                    triggerAlert(rule, metrics);
                }
            } catch (Exception e) {
                log.error("Error evaluating alert rule: {}", rule.getName(), e);
            }
        });
    }
    
    private boolean evaluateCondition(String condition, Map<String, Object> metrics) {
        // Simple condition evaluator - in production use a proper expression evaluator
        if (condition.contains("error_rate >")) {
            Double threshold = Double.parseDouble(condition.split(">")[1].trim());
            Double errorRate = (Double) metrics.getOrDefault("errorRate", 0.0);
            return errorRate > threshold;
        }
        // Add more condition evaluations as needed
        return false;
    }
    
    private void triggerAlert(AlertRule rule, Map<String, Object> metrics) {
        String alertKey = rule.getName() + "_" + rule.getSeverity();
        Instant now = Instant.now();
        
        // Check cooldown
        Instant lastTriggered = lastAlertTime.get(alertKey);
        if (lastTriggered != null && 
            lastTriggered.plusSeconds(rule.getCooldownMinutes() * 60).isAfter(now)) {
            return;
        }
        
        Alert alert = new Alert();
        alert.setId(java.util.UUID.randomUUID().toString());
        alert.setRuleName(rule.getName());
        alert.setSeverity(rule.getSeverity());
        alert.setMessage(String.format("Alert triggered: %s - %s", 
            rule.getName(), rule.getCondition()));
        alert.setTimestamp(now);
        alert.setMetrics(metrics);
        
        log.warn("ALERT: {}", alert.getMessage());
        
        // Send to configured channels
        rule.getActions().forEach(action -> {
            switch (action.toLowerCase()) {
                case "slack":
                    sendSlackAlert(alert);
                    break;
                case "teams":
                    sendTeamsAlert(alert);
                    break;
                case "email":
                    sendEmailAlert(alert);
                    break;
            }
        });
        
        lastAlertTime.put(alertKey, now);
    }
    
    private void sendSlackAlert(Alert alert) {
        if (slackWebhook.isEmpty()) return;
        
        try {
            Map<String, Object> slackMessage = Map.of(
                "text", String.format("*%s Alert*: %s", 
                    alert.getSeverity(), alert.getMessage()),
                "attachments", List.of(Map.of(
                    "color", getColorForSeverity(alert.getSeverity()),
                    "fields", List.of(
                        Map.of("title", "Rule", "value", alert.getRuleName(), "short", true),
                        Map.of("title", "Timestamp", "value", alert.getTimestamp().toString(), "short", true)
                    )
                ))
            );
            
            webClient.post()
                .uri(slackWebhook)
                .bodyValue(slackMessage)
                .retrieve()
                .bodyToMono(Void.class)
                .subscribe();
        } catch (Exception e) {
            log.error("Failed to send Slack alert", e);
        }
    }
    
    private String getColorForSeverity(String severity) {
        switch (severity.toUpperCase()) {
            case "CRITICAL": return "#FF0000";
            case "WARNING": return "#FFA500";
            default: return "#00FF00";
        }
    }
    
    private void sendTeamsAlert(Alert alert) {
        if (teamsWebhook.isEmpty()) return;
        
        // Similar implementation for Microsoft Teams
    }
    
    private void sendEmailAlert(Alert alert) {
        // Email implementation
    }
    
    @Scheduled(fixedRate = 60000) // Check every minute
    public void checkForStaleAlerts() {
        Instant cutoff = Instant.now().minusSeconds(3600); // 1 hour
        lastAlertTime.entrySet().removeIf(entry -> entry.getValue().isBefore(cutoff));
    }
}
