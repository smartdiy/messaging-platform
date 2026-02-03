package com.company.messaging.core.model;

import lombok.Data;
import java.time.Instant;
import java.util.Map;

@Data
public class Payload {
    private String id;
    private byte[] data;
    private Map<String, Object> metadata;
    private String sourceProtocol;
    private String targetProtocol;
    private Instant timestamp;
    private PayloadStatus status;
    private Map<String, Object> headers;
}

enum PayloadStatus {
    RECEIVED, PROCESSING, PROCESSED, FAILED, DELIVERED
}
