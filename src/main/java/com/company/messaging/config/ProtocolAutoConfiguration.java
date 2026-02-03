package com.company.messaging.config;

import com.company.messaging.inbound.protocol.InboundProtocol;
import com.company.messaging.outbound.protocol.OutboundProtocol;
import com.company.messaging.plugin.registry.ProtocolRegistry;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.util.List;
import java.util.Map;

@Slf4j
@Configuration
public class ProtocolAutoConfiguration {
    
    @Autowired(required = false)
    private List<InboundProtocol> inboundProtocols;
    
    @Autowired(required = false)
    private List<OutboundProtocol> outboundProtocols;
    
    @Autowired
    @Lazy
    private ProtocolRegistry protocolRegistry;
    
    @PostConstruct
    public void autoRegisterProtocols() {
        if (inboundProtocols != null) {
            inboundProtocols.forEach(protocol -> {
                protocolRegistry.registerInboundProtocol(protocol);
                log.info("Auto-registered inbound protocol: {}", protocol.getType());
            });
        }
        
        if (outboundProtocols != null) {
            outboundProtocols.forEach(protocol -> {
                protocolRegistry.registerOutboundProtocol(protocol);
                log.info("Auto-registered outbound protocol: {}", protocol.getType());
            });
        }
    }
}
