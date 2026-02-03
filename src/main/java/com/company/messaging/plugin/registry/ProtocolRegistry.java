package com.company.messaging.plugin.registry;

import com.company.messaging.inbound.protocol.InboundProtocol;
import com.company.messaging.outbound.protocol.OutboundProtocol;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ProtocolRegistry {
    private final Map<String, InboundProtocol> inboundProtocols = new ConcurrentHashMap<>();
    private final Map<String, OutboundProtocol> outboundProtocols = new ConcurrentHashMap<>();
    
    public void registerInboundProtocol(InboundProtocol protocol) {
        inboundProtocols.put(protocol.getType(), protocol);
    }
    
    public void registerOutboundProtocol(OutboundProtocol protocol) {
        outboundProtocols.put(protocol.getType(), protocol);
    }
    
    public InboundProtocol getInboundProtocol(String type) {
        return inboundProtocols.get(type);
    }
    
    public OutboundProtocol getOutboundProtocol(String type) {
        return outboundProtocols.get(type);
    }
    
    public Collection<String> getAvailableInboundProtocols() {
        return inboundProtocols.keySet();
    }
    
    public Collection<String> getAvailableOutboundProtocols() {
        return outboundProtocols.keySet();
    }
}
