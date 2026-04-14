package osmos.commerce.marketplace.orderorchestrator.common.utils;

import java.math.BigInteger;
import java.util.Map;

import datadog.trace.api.CorrelationIdentifier;
import datadog.trace.api.DDSpanId;
import datadog.trace.api.DDTraceId;
import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.util.GlobalTracer;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper to extract and activate Datadog trace context from incoming PubSub message attributes.
 * This ensures that the dd.trace_id in logs matches the originating artifact's trace,
 * providing end-to-end trace continuity across PubSub-connected microservices.
 */
@Slf4j
public class PubSubTraceContextHelper {

    private static final String X_DATADOG_TRACE_ID = "x-datadog-trace-id";
    private static final String X_DATADOG_PARENT_ID = "x-datadog-parent-id";
    private static final String X_DATADOG_SAMPLING_PRIORITY = "x-datadog-sampling-priority";

    /**
     * Extracts trace context from PubSub message attributes and activates a child span.
     * Call this at the beginning of message processing.
     * The returned Scope should be closed when processing is complete.
     *
     * @param attributes PubSub message attributes map
     * @return Scope that must be closed after processing, or null if no trace context found
     */
    public static Scope activateTraceFromPubSub(Map<String, String> attributes) {
        if (attributes == null) {
            return null;
        }

        String traceId = attributes.get(X_DATADOG_TRACE_ID);
        String parentId = attributes.get(X_DATADOG_PARENT_ID);

        if (traceId == null || traceId.isEmpty() || "0".equals(traceId)) {
            log.debug("No x-datadog-trace-id found in PubSub attributes, skipping trace activation");
            return null;
        }

        try {
            Tracer tracer = GlobalTracer.get();
            SpanContext extractedContext = tracer.extract(
                    Format.Builtin.TEXT_MAP,
                    new TextMapAdapter(attributes)
            );

            if (extractedContext != null) {
                Span childSpan = tracer.buildSpan("pubsub.consume")
                        .asChildOf(extractedContext)
                        .start();
                Scope scope = tracer.activateSpan(childSpan);

                log.info("Activated trace context from PubSub: x-datadog-trace-id={}, x-datadog-parent-id={}, new dd.trace_id={}",
                        traceId, parentId, CorrelationIdentifier.getTraceId());

                return scope;
            } else {
                log.warn("Could not extract trace context from PubSub attributes despite trace_id={}", traceId);
            }
        } catch (Exception e) {
            log.warn("Failed to activate trace context from PubSub attributes: {}", e.getMessage());
        }

        return null;
    }

    /**
     * Closes the scope and finishes the span if active.
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
