package osmos.commerce.marketplace.orderorchestrator.common.utils;

import java.util.HashMap;
import java.util.Map;

import datadog.trace.api.CorrelationIdentifier;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.util.GlobalTracer;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper to extract Datadog trace context from incoming PubSub message attributes
 * and activate it as the current trace, ensuring trace continuity across services.
 */
@Slf4j
public class PubSubTraceContextHelper {

    private static final String X_DATADOG_TRACE_ID = "x-datadog-trace-id";
    private static final String X_DATADOG_PARENT_ID = "x-datadog-parent-id";
    private static final String X_DATADOG_SAMPLING_PRIORITY = "x-datadog-sampling-priority";
    private static final String DD_TRACE_ID = "dd.trace_id";

    private PubSubTraceContextHelper() {
    }

    /**
     * Extracts trace context from PubSub message attributes and activates a child span.
     * Returns a Scope that should be closed via {@link #deactivateTrace(Scope)} when done.
     */
    public static Scope activateTraceFromPubSub(Map<String, String> attributes) {
        if (attributes == null) {
            return null;
        }

        // Build carrier map with standard Datadog headers
        Map<String, String> carrier = new HashMap<>();

        // Support both "x-datadog-trace-id" (standard) and "dd.trace_id" (legacy) formats
        String traceId = attributes.get(X_DATADOG_TRACE_ID);
        if (traceId == null || traceId.isEmpty()) {
            traceId = attributes.get(DD_TRACE_ID);
        }

        if (traceId == null || traceId.isEmpty() || "0".equals(traceId)) {
            log.debug("No trace ID found in PubSub attributes, skipping trace activation");
            return null;
        }

        // Map to standard Datadog propagation headers
        carrier.put(X_DATADOG_TRACE_ID, traceId);
        carrier.put(X_DATADOG_PARENT_ID,
                attributes.getOrDefault(X_DATADOG_PARENT_ID,
                        attributes.getOrDefault("dd.span_id", "0")));
        carrier.put(X_DATADOG_SAMPLING_PRIORITY,
                attributes.getOrDefault(X_DATADOG_SAMPLING_PRIORITY, "1"));

        log.info("Extracting trace context from PubSub: x-datadog-trace-id={}, x-datadog-parent-id={}",
                carrier.get(X_DATADOG_TRACE_ID), carrier.get(X_DATADOG_PARENT_ID));

        try {
            Tracer tracer = GlobalTracer.get();
            SpanContext extractedContext = tracer.extract(
                    Format.Builtin.TEXT_MAP, new TextMapAdapter(carrier));

            if (extractedContext != null) {
                Span childSpan = tracer.buildSpan("pubsub.consume")
                        .asChildOf(extractedContext)
                        .start();
                Scope scope = tracer.activateSpan(childSpan);
                log.info("Activated propagated trace context: parent_trace_id={}, new dd.trace_id={}, dd.span_id={}",
                        traceId, CorrelationIdentifier.getTraceId(), CorrelationIdentifier.getSpanId());
                return scope;
            } else {
                log.warn("Could not extract trace context from PubSub attributes (tracer returned null), " +
                        "trace_id={}", traceId);
                return null;
            }
        } catch (Exception e) {
            log.warn("Error activating trace context from PubSub: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Deactivates the trace scope and finishes the span.
     * Safe to call with null.
     */
    public static void deactivateTrace(Scope scope) {
        if (scope != null) {
            try {
                Span span = GlobalTracer.get().activeSpan();
                scope.close();
                if (span != null) {
                    span.finish();
                }
            } catch (Exception e) {
                log.warn("Error deactivating trace scope: {}", e.getMessage());
            }
        }
    }
}
