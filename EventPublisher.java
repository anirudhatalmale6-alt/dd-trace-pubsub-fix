package osmos.commerce.sellerorder.publish;

import java.util.List;
import java.util.Map;

import com.google.cloud.spring.pubsub.core.PubSubDeliveryException;
import com.google.cloud.spring.pubsub.core.publisher.PubSubPublisherTemplate;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import datadog.trace.api.CorrelationIdentifier;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import osmos.commerce.common.util.ToJsonConverter;
import osmos.commerce.marketplace.common.publish.PubSubOrderingKeyPublisherTemplate;
import osmos.commerce.sellerorder.exception.RetryableException;

import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.apache.commons.text.StringEscapeUtils.escapeJava;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventPublisher {

    private final PubSubPublisherTemplate pubSubPublisherTemplate;
    private final PubSubOrderingKeyPublisherTemplate pubSubOrderingKeyPublisherTemplate;
    private final ToJsonConverter toJsonConverter;

    @SneakyThrows
    public void publishToPubSub(List<String> topicNames, Object data, Map<String, String> headers) {
        String value = toJsonConverter.convert(data);
        injectTraceContext(headers);
        topicNames.forEach(topic -> pubSubPublisherTemplate.publish(topic, value, headers));
        log.info("Event published with value :: {} and headers :: {} and topic names :: {}",
                value, headers, topicNames);
    }

    @SneakyThrows
    public void publishEventWithOrderingKey(List<String> topicNames, Object data, Map<String, String> headers, String orderingKey) {
        log.info("Publishing event with ordering key as enabled");
        String value = toJsonConverter.convert(data);
        headers.values().removeAll(singleton(null));
        injectTraceContext(headers);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(value))
                .setOrderingKey(orderingKey)
                .putAllAttributes(headers)
                .build();

        Map<String, String> contextMap = MDC.getCopyOfContextMap();

        topicNames.forEach(topic -> {
            try {
                MDC.setContextMap(contextMap);
                ListenableFuture<String> listenableFuture = pubSubOrderingKeyPublisherTemplate.publish(topic, pubsubMessage);
                log.info(escapeJava(format("Successfully published with message id :: %s", listenableFuture.get())));
            } catch (Exception e) {
                log.error("Failed to publish message with ordering key. ", e);
                throw new RetryableException(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
            }
        });
        log.info("Event published with value :: {} and headers :: {} and topic names :: {} and ordering key :: {}",
                value, headers, topicNames, orderingKey);
    }

    @SneakyThrows
    @Retryable(maxAttempts = 2, value = PubSubDeliveryException.class)
    public void publishToPubSubWithOneRetry(String topicName, String value, Map<String, String> headers) {
        if (RetrySynchronizationManager.getContext().getRetryCount() > 0) {
            log.warn("Publishing message to the topic {} failed. Retrying now, details : {}",
                    topicName, RetrySynchronizationManager.getContext());
        }
        injectTraceContext(headers);
        pubSubPublisherTemplate.publish(topicName, value, headers).get();
    }

    /**
     * Injects Datadog trace context (trace_id, parent_id, sampling_priority)
     * into PubSub message headers so downstream artifacts can continue the trace.
     */
    private void injectTraceContext(Map<String, String> headers) {
        String traceId = CorrelationIdentifier.getTraceId();
        String spanId = CorrelationIdentifier.getSpanId();

        if (traceId != null && !traceId.equals("0") && !headers.containsKey("x-datadog-trace-id")) {
            headers.put("x-datadog-trace-id", traceId);
            headers.put("x-datadog-parent-id", spanId != null ? spanId : "0");
            headers.put("x-datadog-sampling-priority", "1");
            log.info("Injected trace context into PubSub headers: x-datadog-trace-id={}, x-datadog-parent-id={}", traceId, spanId);
        }
    }

}
