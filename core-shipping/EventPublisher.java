package osmos.commerce.shipping.publish;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import datadog.trace.api.CorrelationIdentifier;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gcp.pubsub.core.publisher.PubSubPublisherTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import osmos.commerce.common.exception.RetryableException;
import osmos.commerce.common.util.ToJsonConverter;
import osmos.commerce.marketplace.common.publish.PubSubOrderingKeyPublisherTemplate;

import static java.lang.String.format;
import static java.util.Collections.singleton;
import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static osmos.commerce.common.util.ApplicationConstants.ATTRIBUTE_EVENT_TYPE;
import static osmos.commerce.common.util.ApplicationConstants.AUTHORIZATION;
import static osmos.commerce.common.util.ApplicationConstants.X_TENANT_ID;
import static osmos.commerce.shipping.publish.Publisher.TENANT_COUNTRY_ENV_MAPPING;
import static osmos.commerce.shipping.util.ApplicationConstants.COUNTRY;

@Service
@RequiredArgsConstructor
@Slf4j
public class EventPublisher {

    private final PubSubPublisherTemplate pubSubPublisherTemplate;
    private final PubSubOrderingKeyPublisherTemplate pubSubOrderingKeyPublisherTemplate;
    private final ObjectMapper objectMapper;
    private final ToJsonConverter toJsonConverter;

    @SneakyThrows
    public void publishToPubSub(List<String> topics, Object data, Map<String, String> headers) {
        updateCountryAttributeInHeaders(headers);
        injectTraceContext(headers);
        String value = objectMapper.writeValueAsString(data);
        topics.forEach(topicName -> pubSubPublisherTemplate.publish(topicName, value, headers));
        log.info("Event published with value :: {} and headers :: {} and topic names :: {}",
                value, headers, topics);
    }

    @SneakyThrows
    public void publishEventWithOrderingKey(List<String> topics, Object data, Map<String, String> headers, String orderingKey) {
        log.info("Publishing event with ordering key as enabled");
        String value = toJsonConverter.convert(data);
        updateCountryAttributeInHeaders(headers);
        headers.values().removeAll(singleton(null));
        headers.entrySet().removeIf(entry -> AUTHORIZATION.equalsIgnoreCase(entry.getKey()));
        injectTraceContext(headers);

        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8(value))
                .setOrderingKey(orderingKey)
                .putAllAttributes(headers)
                .build();
        topics.forEach(topic -> {
            try {
                ListenableFuture<String> listenableFuture = pubSubOrderingKeyPublisherTemplate.publish(topic, pubsubMessage);
                log.info(escapeJava(format("Successfully published with message id :: %s", listenableFuture.get())));
            } catch (Exception e) {
                log.error("Failed to publish message with ordering key. ", e);
                throw new RetryableException(e);
            }
        });
        log.info("Event published with eventType :: {} and value :: {} and headers :: {} and topic names :: {} and ordering key :: {}",
                headers.get(ATTRIBUTE_EVENT_TYPE), value, headers, topics, orderingKey);
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

    private static void updateCountryAttributeInHeaders(Map<String, String> headers) {
        if (headers.get(COUNTRY) == null) {
            String tenantId = headers.get(X_TENANT_ID);
            if (tenantId != null && TENANT_COUNTRY_ENV_MAPPING.containsKey(tenantId)) {
                String[] countryEnvArray = TENANT_COUNTRY_ENV_MAPPING.get(tenantId);
                if (countryEnvArray != null && countryEnvArray.length > 0) {
                    headers.put(COUNTRY, countryEnvArray[0]);
                }
            }
        }
    }
}
