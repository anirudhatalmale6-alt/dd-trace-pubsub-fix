package osmos.commerce.order.publish;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import datadog.trace.api.CorrelationIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.gcp.pubsub.core.publisher.PubSubPublisherTemplate;
import osmos.commerce.common.util.ToJsonConverter;
import osmos.commerce.order.event.ordering.PubSubOrderingKeyPublisherTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventPublisherTest {

    @Mock
    private PubSubPublisherTemplate pubSubPublisherTemplate;

    @Mock
    private PubSubOrderingKeyPublisherTemplate pubSubOrderingKeyPublisherTemplate;

    @Mock
    private ToJsonConverter toJsonConverter;

    @InjectMocks
    private EventPublisher eventPublisher;

    @Captor
    private ArgumentCaptor<Map<String, String>> headersCaptor;

    @Captor
    private ArgumentCaptor<PubsubMessage> pubsubMessageCaptor;

    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_VALUE = "{\"key\":\"value\"}";
    private static final String TEST_TRACE_ID = "1234567890";
    private static final String TEST_SPAN_ID = "9876543210";

    @BeforeEach
    void setUp() {
        when(toJsonConverter.convert(any())).thenReturn(TEST_VALUE);
    }

    // ==================== publishToPubSub tests ====================

    @Test
    void publishToPubSub_shouldPublishToAllTopics() {
        List<String> topics = List.of("topic-1", "topic-2");
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer token");
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishToPubSub(topics, data, headers);
        }

        verify(pubSubPublisherTemplate, times(2)).publish(anyString(), eq(TEST_VALUE), any());
        assertThat(headers).doesNotContainKey("Authorization");
    }

    @Test
    void publishToPubSub_shouldInjectTraceContext() {
        List<String> topics = List.of(TEST_TOPIC);
        Map<String, String> headers = new HashMap<>();
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishToPubSub(topics, data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> capturedHeaders = headersCaptor.getValue();
        assertThat(capturedHeaders).containsEntry("dd.trace_id", TEST_TRACE_ID);
        assertThat(capturedHeaders).containsEntry("x-datadog-trace-id", TEST_TRACE_ID);
        assertThat(capturedHeaders).containsEntry("x-datadog-parent-id", TEST_SPAN_ID);
        assertThat(capturedHeaders).containsEntry("x-datadog-sampling-priority", "1");
    }

    @Test
    void publishToPubSub_shouldNotOverwriteExistingTraceHeaders() {
        List<String> topics = List.of(TEST_TOPIC);
        Map<String, String> headers = new HashMap<>();
        headers.put("dd.trace_id", "existing-trace");
        headers.put("x-datadog-trace-id", "existing-x-trace");
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishToPubSub(topics, data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> capturedHeaders = headersCaptor.getValue();
        assertThat(capturedHeaders).containsEntry("dd.trace_id", "existing-trace");
        assertThat(capturedHeaders).containsEntry("x-datadog-trace-id", "existing-x-trace");
    }

    @Test
    void publishToPubSub_shouldNotInjectTraceWhenTraceIdIsNull() {
        List<String> topics = List.of(TEST_TOPIC);
        Map<String, String> headers = new HashMap<>();
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(null);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(null);

            eventPublisher.publishToPubSub(topics, data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> capturedHeaders = headersCaptor.getValue();
        assertThat(capturedHeaders).doesNotContainKey("dd.trace_id");
        assertThat(capturedHeaders).doesNotContainKey("x-datadog-trace-id");
    }

    @Test
    void publishToPubSub_shouldNotInjectTraceWhenTraceIdIsZero() {
        List<String> topics = List.of(TEST_TOPIC);
        Map<String, String> headers = new HashMap<>();
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn("0");
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn("0");

            eventPublisher.publishToPubSub(topics, data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> capturedHeaders = headersCaptor.getValue();
        assertThat(capturedHeaders).doesNotContainKey("dd.trace_id");
        assertThat(capturedHeaders).doesNotContainKey("x-datadog-trace-id");
    }

    @Test
    void publishToPubSub_shouldHandleNullSpanId() {
        List<String> topics = List.of(TEST_TOPIC);
        Map<String, String> headers = new HashMap<>();
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(null);

            eventPublisher.publishToPubSub(topics, data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> capturedHeaders = headersCaptor.getValue();
        assertThat(capturedHeaders).containsEntry("x-datadog-parent-id", "0");
    }

    // ==================== publishToPubSubWithOrderingKey tests ====================

    @Test
    void publishToPubSubWithOrderingKey_shouldPublishWithOrderingKey() {
        List<String> topics = List.of(TEST_TOPIC);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer token");
        Object data = Map.of("key", "value");
        String orderingKey = "order-123";

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishToPubSubWithOrderingKey(topics, data, headers, orderingKey);
        }

        verify(pubSubOrderingKeyPublisherTemplate).publish(eq(TEST_TOPIC), pubsubMessageCaptor.capture());
        PubsubMessage capturedMessage = pubsubMessageCaptor.getValue();
        assertThat(capturedMessage.getOrderingKey()).isEqualTo(orderingKey);
        assertThat(capturedMessage.getData()).isEqualTo(ByteString.copyFromUtf8(TEST_VALUE));
        assertThat(capturedMessage.getAttributesMap()).containsEntry("dd.trace_id", TEST_TRACE_ID);
        assertThat(capturedMessage.getAttributesMap()).containsEntry("x-datadog-trace-id", TEST_TRACE_ID);
        assertThat(headers).doesNotContainKey("Authorization");
    }

    @Test
    void publishToPubSubWithOrderingKey_shouldRemoveNullValues() {
        List<String> topics = List.of(TEST_TOPIC);
        Map<String, String> headers = new HashMap<>();
        headers.put("valid-header", "value");
        headers.put("null-header", null);
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishToPubSubWithOrderingKey(topics, data, headers, "key-1");
        }

        verify(pubSubOrderingKeyPublisherTemplate).publish(eq(TEST_TOPIC), any(PubsubMessage.class));
        assertThat(headers).doesNotContainValue(null);
    }

    // ==================== publishToDLQ tests ====================

    @Test
    void publishToDLQ_shouldPublishToSingleTopic() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer token");
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishToDLQ("dlq-topic", data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq("dlq-topic"), eq(TEST_VALUE), any());
        assertThat(headers).doesNotContainKey("Authorization");
    }

    @Test
    void publishToDLQ_shouldInjectTraceContext() {
        Map<String, String> headers = new HashMap<>();
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishToDLQ("dlq-topic", data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq("dlq-topic"), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> capturedHeaders = headersCaptor.getValue();
        assertThat(capturedHeaders).containsEntry("dd.trace_id", TEST_TRACE_ID);
    }

    // ==================== publishReturnOrderToErrorTopic tests ====================

    @Test
    void publishReturnOrderToErrorTopic_shouldPublishToErrorTopic() {
        Map<String, String> headers = new HashMap<>();
        headers.put("valid-header", "value");
        headers.put("null-header", null);
        Object data = Map.of("returnRequestId", "123");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishReturnOrderToErrorTopic(data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq("return-request-error"), eq(TEST_VALUE), any());
    }

    // ==================== injectTraceContext edge cases ====================

    @Test
    void publishToPubSub_shouldNotInjectDdTraceIdWhenAlreadyExists() {
        List<String> topics = List.of(TEST_TOPIC);
        Map<String, String> headers = new HashMap<>();
        headers.put("dd.trace_id", "pre-existing-trace");
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishToPubSub(topics, data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> capturedHeaders = headersCaptor.getValue();
        assertThat(capturedHeaders).containsEntry("dd.trace_id", "pre-existing-trace");
        // x-datadog-trace-id should still be injected since it wasn't pre-existing
        assertThat(capturedHeaders).containsEntry("x-datadog-trace-id", TEST_TRACE_ID);
    }

    @Test
    void publishToPubSub_shouldNotInjectXDatadogTraceIdWhenAlreadyExists() {
        List<String> topics = List.of(TEST_TOPIC);
        Map<String, String> headers = new HashMap<>();
        headers.put("x-datadog-trace-id", "pre-existing-x-trace");
        Object data = Map.of("key", "value");

        try (MockedStatic<CorrelationIdentifier> mockedCorrelation = mockStatic(CorrelationIdentifier.class)) {
            mockedCorrelation.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mockedCorrelation.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);

            eventPublisher.publishToPubSub(topics, data, headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> capturedHeaders = headersCaptor.getValue();
        // dd.trace_id should be injected since it wasn't pre-existing
        assertThat(capturedHeaders).containsEntry("dd.trace_id", TEST_TRACE_ID);
        // x-datadog-trace-id should keep original
        assertThat(capturedHeaders).containsEntry("x-datadog-trace-id", "pre-existing-x-trace");
        // x-datadog-parent-id should NOT be set since we skipped the x-datadog block
        assertThat(capturedHeaders).doesNotContainKey("x-datadog-parent-id");
    }
}
