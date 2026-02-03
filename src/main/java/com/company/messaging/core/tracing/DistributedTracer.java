package com.company.messaging.core.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class DistributedTracer {
    
    @Autowired(required = false)
    private Tracer tracer;
    
    @Autowired(required = false)
    private Propagator propagator;
    
    private final Map<String, Span> activeSpans = new ConcurrentHashMap<>();
    
    public TraceContext startTrace(String operation, Map<String, String> tags) {
        TraceContext context = new TraceContext();
        context.setOperation(operation);
        
        if (tracer != null) {
            Span span = tracer.nextSpan()
                .name(operation)
                .start();
            
            tags.forEach(span::tag);
            tracer.withSpan(span);
            
            context.setTraceId(span.context().traceId());
            context.setSpanId(span.context().spanId());
            activeSpans.put(context.getSpanId(), span);
        }
        
        return context;
    }
    
    public void endTrace(TraceContext context) {
        if (tracer != null && context != null) {
            Span span = activeSpans.remove(context.getSpanId());
            if (span != null) {
                span.end();
            }
        }
    }
    
    public ExchangeFilterFunction tracingExchangeFilter() {
        return ExchangeFilterFunction.ofRequestProcessor(request -> {
            if (tracer != null && propagator != null) {
                ClientRequest.Builder builder = ClientRequest.from(request);
                propagator.inject(tracer.currentTraceContext().context(), 
                    builder.headers(), 
                    (headers, key, value) -> headers.add(key, value));
                return Mono.just(builder.build());
            }
            return Mono.just(request);
        });
    }
    
    public void injectTraceHeaders(HttpHeaders headers, TraceContext context) {
        if (tracer != null && propagator != null && context != null) {
            Span span = Span.fromContext(tracer.currentTraceContext().context());
            propagator.inject(span.context(), headers, 
                (h, key, value) -> h.add(key, value));
        }
    }
    
    public TraceContext extractTraceHeaders(HttpHeaders headers) {
        TraceContext context = new TraceContext();
        
        if (tracer != null && propagator != null) {
            io.micrometer.tracing.propagation.Propagator.Getter<HttpHeaders> getter =
                (carrier, key) -> carrier.getFirst(key);
            
            io.micrometer.tracing.TraceContext extracted = 
                propagator.extract(headers, getter);
            
            if (extracted != null) {
                Span span = tracer.nextSpan(extracted).start();
                context.setTraceId(span.context().traceId());
                context.setSpanId(span.context().spanId());
                tracer.withSpan(span);
                activeSpans.put(context.getSpanId(), span);
            }
        }
        
        return context;
    }
    
    public void addEvent(String event, Map<String, String> attributes) {
        if (tracer != null) {
            Span span = tracer.currentSpan();
            if (span != null) {
                span.event(event);
                attributes.forEach(span::tag);
            }
        }
    }
}
