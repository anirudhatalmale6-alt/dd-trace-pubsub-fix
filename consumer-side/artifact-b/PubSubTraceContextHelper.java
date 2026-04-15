package osmos.commerce.sellerorder.util;

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
@SuppressWarnings("PMD.CloseResource")
public class PubSubTraceContextHelper {

    private static final String X_DATADOG_TRACE_ID = "x-datadog-trace-id";
    private static final String X_DATADOG_PARENT_ID = "x-datadog-parent-id";
    private static final String X_DATADOG_SAMPLING_PRIORITY = "x-datadog-sampling-priority";
    private static final String DD_TRACE_ID = "dd.trace_id";
    private static final String EMPTY_TRACE_ID = "0";
    private static final String DEFAULT_SPAN_ID = "0";
    private static final String DEFAULT_SAMPLING_PRIORITY = "1";

    private PubSubTraceContextHelper() {
    }

    /**
     * Extracts trace context from PubSub message attributes and activates a child span.
     * Returns a TraceScope that MUST be closed when processing is complete.
     *
     * Usage:
     * <pre>
     * try (TraceScope traceScope = PubSubTraceContextHelper.activateTraceFromAttributes(
     *         message.getAttributes(), "events.process")) {
     *     // process the message - trace context is active here
     * }
     * </pre>
     */
    public static TraceScope activateTraceFromAttributes(Map<String, String> attributes, String operationName) {
        if (attributes == null) {
            return new TraceScope(null, null);
        }

        // Support both "x-datadog-trace-id" (standard) and "dd.trace_id" (legacy) formats
        String traceId = attributes.get(X_DATADOG_TRACE_ID);
        if (traceId == null || traceId.isEmpty()) {
            traceId = attributes.get(DD_TRACE_ID);
        }

        if (traceId == null || traceId.isEmpty() || EMPTY_TRACE_ID.equals(traceId)) {
            log.debug("No trace ID found in PubSub attributes, skipping trace activation");
            return new TraceScope(null, null);
        }

        // Build carrier map with standard Datadog propagation headers
        Map<String, String> carrier = new HashMap<>();
        carrier.put(X_DATADOG_TRACE_ID, traceId);
        carrier.put(X_DATADOG_PARENT_ID,
                attributes.getOrDefault(X_DATADOG_PARENT_ID,
                        attributes.getOrDefault("dd.span_id", DEFAULT_SPAN_ID)));
        carrier.put(X_DATADOG_SAMPLING_PRIORITY,
                attributes.getOrDefault(X_DATADOG_SAMPLING_PRIORITY, DEFAULT_SAMPLING_PRIORITY));

        log.info("Extracting trace context from PubSub: x-datadog-trace-id={}, x-datadog-parent-id={}",
                carrier.get(X_DATADOG_TRACE_ID), carrier.get(X_DATADOG_PARENT_ID));

        try {
            Tracer tracer = GlobalTracer.get();
            SpanContext extractedContext = tracer.extract(
                    Format.Builtin.TEXT_MAP, new TextMapAdapter(carrier));

            if (extractedContext != null) {
                Span span = tracer.buildSpan(operationName)
                        .asChildOf(extractedContext)
                        .start();
                Scope scope = tracer.activateSpan(span);
                log.info("Activated propagated trace context: parent_trace_id={}, new dd.trace_id={}, dd.span_id={}",
                        traceId, CorrelationIdentifier.getTraceId(), CorrelationIdentifier.getSpanId());
                return new TraceScope(span, scope);
            } else {
                log.warn("Could not extract trace context from PubSub attributes (tracer returned null), trace_id={}", traceId);
                return new TraceScope(null, null);
            }
        } catch (Exception e) {
            log.warn("Error activating trace context from PubSub: {}", e.getMessage());
            return new TraceScope(null, null);
        }
    }

    /**
     * AutoCloseable wrapper that properly finishes the span and closes the scope.
     */
    public static class TraceScope implements AutoCloseable {
        private final Span span;
        private final Scope scope;

        TraceScope(Span span, Scope scope) {
            this.span = span;
            this.scope = scope;
        }

        @Override
        public void close() {
            if (scope != null) {
                try {
                    scope.close();
                } catch (Exception e) {
                    log.trace("Error closing trace scope: {}", e.getMessage());
                }
            }
            if (span != null) {
                try {
                    span.finish();
                } catch (Exception e) {
                    log.trace("Error finishing trace span: {}", e.getMessage());
                }
            }
        }
    }
}
