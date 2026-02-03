// src/main/java/com/company/messaging/config/ProtocolInitializer.java
package com.company.messaging.config;

import com.company.messaging.inbound.protocol.InboundProtocol;
import com.company.messaging.outbound.protocol.OutboundProtocol;
import com.company.messaging.plugin.registry.ProtocolRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Slf4j
@Configuration
@ConfigurationProperties(prefix = "protocols")
public class ProtocolInitializer {
    
    @Autowired
    private ProtocolRegistry protocolRegistry;
    
    private Map<String, Map<String, Object>> inbound;
    private Map<String, Map<String, Object>> outbound;
    
    @PostConstruct
    public void initializeProtocols() {
        // Initialize inbound protocols
        inbound.forEach((type, config) -> {
            InboundProtocol protocol = protocolRegistry.getInboundProtocol(type);
            if (protocol != null) {
                protocol.initialize(config);
                log.info("Initialized inbound protocol: {}", type);
            }
        });
        
        // Initialize outbound protocols
        outbound.forEach((type, config) -> {
            OutboundProtocol protocol = protocolRegistry.getOutboundProtocol(type);
            if (protocol != null) {
                protocol.initialize(config);
                log.info("Initialized outbound protocol: {}", type);
            }
        });
    }
    
    // Getters and setters
    public Map<String, Map<String, Object>> getInbound() {
        return inbound;
    }
    
    public void setInbound(Map<String, Map<String, Object>> inbound) {
        this.inbound = inbound;
    }
    
    public Map<String, Map<String, Object>> getOutbound() {
        return outbound;
    }
    
    public void setOutbound(Map<String, Map<String, Object>> outbound) {
        this.outbound = outbound;
    }
}
