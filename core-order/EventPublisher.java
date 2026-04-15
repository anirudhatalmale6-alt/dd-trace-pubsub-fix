package osmos.commerce.order.publish;

import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import datadog.trace.api.CorrelationIdentifier;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gcp.pubsub.core.publisher.PubSubPublisherTemplate;
import org.springframework.stereotype.Service;
import osmos.commerce.common.util.ToJsonConverter;
import osmos.commerce.order.event.ordering.PubSubOrderingKeyPublisherTemplate;

import static java.util.Collections.singleton;
import static osmos.commerce.common.util.ApplicationConstants.AUTHORIZATION;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventPublisher {

    private final PubSubPublisherTemplate pubSubPublisherTemplate;
    private final PubSubOrderingKeyPublisherTemplate pubSubOrderingKeyPublisherTemplate;
    private final ToJsonConverter toJsonConverter;

    @SneakyThrows
    public void publishToPubSub(List<String> topics, Object data, Map<String, String> headers) {
        var value = toJsonConverter.convert(data);
        injectTraceContext(headers);
        topics.forEach(topicName -> pubSubPublisherTemplate.publish(topicName, value, headers));
        headers.remove(AUTHORIZATION);
        log.info("Event published with value :: {} and headers :: {} and topic names :: {}", value, headers, topics);
    }

    @SneakyThrows
    public void publishToPubSubWithOrderingKey(List<String> topics, Object data, Map<String, String> headers, String orderingKey) {
        log.info("Publishing event with ordering key as enabled with orderingKey: {}", orderingKey);
        String value = toJsonConverter.convert(data);
        headers.values().removeAll(singleton(null));
        injectTraceContext(headers);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(value))
                .setOrderingKey(orderingKey)
                .putAllAttributes(headers)
                .build();
        topics.forEach(topicName -> pubSubOrderingKeyPublisherTemplate.publish(topicName, pubsubMessage));
        headers.remove(AUTHORIZATION);
        log.info("Event published with value :: {} and headers :: {} and topic names :: {} and ordering key :: {}",
                value, headers, topics, orderingKey);
    }

    @SneakyThrows
    public void publishToDLQ(String topicName, Object data, Map<String, String> headers) {
        var value = toJsonConverter.convert(data);
        injectTraceContext(headers);
        pubSubPublisherTemplate.publish(topicName, value, headers);
        headers.remove(AUTHORIZATION);
        log.info("Event published with value :: {} and headers :: {} and topic names :: {}", value, headers, topicName);
    }

    public void publishReturnOrderToErrorTopic(Object data, Map<String, String> headers) {
         headers.values().removeAll(singleton(null));
         publishToPubSub(List.of("return-request-error"), data, headers);
         log.info("returnRequestId published to return-request-error topic :: {}", data);
    }

    /**
     * Injects Datadog trace context (dd.trace_id, x-datadog-trace-id, x-datadog-parent-id,
     * x-datadog-sampling-priority) into PubSub message headers so downstream artifacts
     * can continue the trace.
     */
    private void injectTraceContext(Map<String, String> headers) {
        String traceId = CorrelationIdentifier.getTraceId();
        String spanId = CorrelationIdentifier.getSpanId();

        if (traceId != null && !traceId.equals("0")) {
            if (!headers.containsKey("dd.trace_id")) {
                headers.put("dd.trace_id", traceId);
            }
            if (!headers.containsKey("x-datadog-trace-id")) {
                headers.put("x-datadog-trace-id", traceId);
                headers.put("x-datadog-parent-id", spanId != null ? spanId : "0");
                headers.put("x-datadog-sampling-priority", "1");
            }
            log.info("Injected trace context into PubSub headers: dd.trace_id={}, x-datadog-trace-id={}, x-datadog-parent-id={}",
                    traceId, traceId, spanId);
        }
    }

}
