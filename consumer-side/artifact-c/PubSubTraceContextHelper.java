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
     * Functional interface for operations that may throw checked exceptions.
     */
    @FunctionalInterface
    public interface TracedOperation<T> {
        T execute() throws Exception;
    }

    /**
     * Functional interface for void operations that may throw checked exceptions.
     */
    @FunctionalInterface
    public interface TracedRunnable {
        void run() throws Exception;
    }

    /**
     * Executes an operation within the propagated trace context extracted from PubSub attributes.
     * Activates the trace before execution and deactivates it after, regardless of outcome.
     *
     * Usage:
     * <pre>
     * ResponseEntity response = PubSubTraceContextHelper.executeWithTrace(
     *     message.getAttributes(), "events.process",
     *     () -> {
     *         eventHandlerStrategyDelegator.determineAndCallStrategy(pubsubRequest, headers);
     *         return ResponseEntity.status(OK).build();
     *     });
     * </pre>
     */
    public static <T> T executeWithTrace(Map<String, String> attributes, String operationName,
                                          TracedOperation<T> operation) throws Exception {
        Span span = null;
        Scope scope = null;
        try {
            SpanContext extractedContext = extractTraceContext(attributes);
            if (extractedContext != null) {
                Tracer tracer = GlobalTracer.get();
                span = tracer.buildSpan(operationName).asChildOf(extractedContext).start();
                scope = tracer.activateSpan(span);
                if (log.isInfoEnabled()) {
                    log.info("Activated propagated trace context: parent_trace_id={}, new dd.trace_id={}, dd.span_id={}",
                            resolveTraceId(attributes), CorrelationIdentifier.getTraceId(), CorrelationIdentifier.getSpanId());
                }
            }
            return operation.execute();
        } finally {
            deactivate(scope, span);
        }
    }

    /**
     * Executes a void operation within the propagated trace context.
     *
     * Usage:
     * <pre>
     * PubSubTraceContextHelper.runWithTrace(
     *     message.getAttributes(), "events.process",
     *     () -> eventHandlerStrategyDelegator.determineAndCallStrategy(pubsubRequest, headers));
     * </pre>
     */
    public static void runWithTrace(Map<String, String> attributes, String operationName,
                                     TracedRunnable operation) throws Exception {
        executeWithTrace(attributes, operationName, () -> {
            operation.run();
            return null;
        });
    }

    private static SpanContext extractTraceContext(Map<String, String> attributes) {
        if (attributes == null) {
            return null;
        }

        String traceId = resolveTraceId(attributes);
        if (traceId == null || traceId.isEmpty() || EMPTY_TRACE_ID.equals(traceId)) {
            log.debug("No trace ID found in PubSub attributes, skipping trace activation");
            return null;
        }

        Map<String, String> carrier = new HashMap<>();
        carrier.put(X_DATADOG_TRACE_ID, traceId);
        carrier.put(X_DATADOG_PARENT_ID,
                attributes.getOrDefault(X_DATADOG_PARENT_ID,
                        attributes.getOrDefault("dd.span_id", DEFAULT_SPAN_ID)));
        carrier.put(X_DATADOG_SAMPLING_PRIORITY,
                attributes.getOrDefault(X_DATADOG_SAMPLING_PRIORITY, DEFAULT_SAMPLING_PRIORITY));

        if (log.isInfoEnabled()) {
            log.info("Extracting trace context from PubSub: x-datadog-trace-id={}, x-datadog-parent-id={}",
                    carrier.get(X_DATADOG_TRACE_ID), carrier.get(X_DATADOG_PARENT_ID));
        }

        return GlobalTracer.get().extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(carrier));
    }

    private static String resolveTraceId(Map<String, String> attributes) {
        String traceId = attributes.get(X_DATADOG_TRACE_ID);
        if (traceId == null || traceId.isEmpty()) {
            traceId = attributes.get(DD_TRACE_ID);
        }
        return traceId;
    }

    private static void deactivate(Scope scope, Span span) {
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
