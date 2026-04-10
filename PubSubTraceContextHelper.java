package osmos.commerce.sellerorder.util;

import java.util.HashMap;
import java.util.Map;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.util.GlobalTracer;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper to extract Datadog trace context from PubSub message attributes
 * and activate it so the dd-java-agent manages MDC correctly.
 */
@Slf4j
public class PubSubTraceContextHelper {

    private PubSubTraceContextHelper() {
    }

    /**
     * Activates trace context from PubSub attributes.
     * Returns a TraceScope that MUST be closed when processing is done.
     * Use in a try-with-resources or try/finally block.
     */
    public static TraceScope activateTraceFromAttributes(Map<String, String> attributes, String operationName) {
        Tracer tracer = GlobalTracer.get();

        Map<String, String> carrier = new HashMap<>(attributes);

        // Support both the old "dd.trace_id" format and the standard "x-datadog-trace-id" format
        if (carrier.containsKey("dd.trace_id") && !carrier.containsKey("x-datadog-trace-id")) {
            carrier.put("x-datadog-trace-id", carrier.get("dd.trace_id"));
            carrier.put("x-datadog-parent-id", carrier.getOrDefault("dd.span_id", "0"));
            carrier.put("x-datadog-sampling-priority", "1");
        }

        String traceId = carrier.getOrDefault("x-datadog-trace-id", carrier.get("dd.trace_id"));
        log.info("Activating propagated trace context: trace_id={}", traceId);

        SpanContext extractedContext = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(carrier));

        Span span;
        if (extractedContext != null) {
            span = tracer.buildSpan(operationName)
                    .asChildOf(extractedContext)
                    .start();
            log.info("Created child span continuing trace from artifact A");
        } else {
            span = tracer.buildSpan(operationName).start();
            log.warn("Could not extract trace context from attributes, starting new trace");
        }

        Scope scope = tracer.activateSpan(span);
        return new TraceScope(span, scope);
    }

    /**
     * Holds the active span and scope so they can be properly closed.
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
            try {
                span.finish();
            } finally {
                scope.close();
            }
        }
    }
}
