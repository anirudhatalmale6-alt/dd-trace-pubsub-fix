package osmos.commerce.order.publish;

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

    private static final String TEST_TOPIC = "test-topic";
    private static final String TEST_VALUE = "{\"data\":\"payload\"}";
    private static final String TEST_TRACE_ID = "1234567890";
    private static final String TEST_SPAN_ID = "9876543210";
    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_DD_TRACE_ID = "dd.trace_id";
    private static final String HEADER_X_DATADOG_TRACE_ID = "x-datadog-trace-id";
    private static final String HEADER_X_DATADOG_PARENT_ID = "x-datadog-parent-id";
    private static final String HEADER_X_DATADOG_SAMPLING = "x-datadog-sampling-priority";
    private static final String DLQ_TOPIC = "dlq-topic";
    private static final String DATA_KEY = "data";
    private static final String DATA_VALUE = "payload";
    private static final String BEARER_TOKEN = "Bearer token";
    private static final String PRE_EXISTING = "pre-existing";
    private static final String ZERO = "0";

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

    @BeforeEach
    void setUp() {
        when(toJsonConverter.convert(any())).thenReturn(TEST_VALUE);
    }

    private Map<String, String> createHeaders() {
        return new HashMap<>();
    }

    private Map<String, String> createHeadersWithAuth() {
        Map<String, String> headers = new HashMap<>();
        headers.put(HEADER_AUTHORIZATION, BEARER_TOKEN);
        return headers;
    }

    private Object createData() {
        return Map.of(DATA_KEY, DATA_VALUE);
    }

    // ==================== publishToPubSub ====================

    @Test
    void shouldPublishToAllTopics() {
        List<String> topics = List.of("topic-1", "topic-2");
        Map<String, String> headers = createHeadersWithAuth();

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishToPubSub(topics, createData(), headers);
        }

        verify(pubSubPublisherTemplate, times(2)).publish(anyString(), eq(TEST_VALUE), any());
        assertThat(headers).doesNotContainKey(HEADER_AUTHORIZATION);
    }

    @Test
    void shouldInjectTraceContextWhenPublishing() {
        Map<String, String> headers = createHeaders();

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishToPubSub(List.of(TEST_TOPIC), createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> captured = headersCaptor.getValue();
        assertThat(captured).containsEntry(HEADER_DD_TRACE_ID, TEST_TRACE_ID);
        assertThat(captured).containsEntry(HEADER_X_DATADOG_TRACE_ID, TEST_TRACE_ID);
        assertThat(captured).containsEntry(HEADER_X_DATADOG_PARENT_ID, TEST_SPAN_ID);
        assertThat(captured).containsEntry(HEADER_X_DATADOG_SAMPLING, "1");
    }

    @Test
    void shouldNotOverwriteExistingTraceHeaders() {
        Map<String, String> headers = createHeaders();
        headers.put(HEADER_DD_TRACE_ID, PRE_EXISTING);
        headers.put(HEADER_X_DATADOG_TRACE_ID, PRE_EXISTING);

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishToPubSub(List.of(TEST_TOPIC), createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> captured = headersCaptor.getValue();
        assertThat(captured).containsEntry(HEADER_DD_TRACE_ID, PRE_EXISTING);
        assertThat(captured).containsEntry(HEADER_X_DATADOG_TRACE_ID, PRE_EXISTING);
    }

    @Test
    void shouldSkipTraceInjectionWhenTraceIdIsNull() {
        Map<String, String> headers = createHeaders();

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(null);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(null);
            eventPublisher.publishToPubSub(List.of(TEST_TOPIC), createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        assertThat(headersCaptor.getValue()).doesNotContainKey(HEADER_DD_TRACE_ID);
        assertThat(headersCaptor.getValue()).doesNotContainKey(HEADER_X_DATADOG_TRACE_ID);
    }

    @Test
    void shouldSkipTraceInjectionWhenTraceIdIsZero() {
        Map<String, String> headers = createHeaders();

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(ZERO);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(ZERO);
            eventPublisher.publishToPubSub(List.of(TEST_TOPIC), createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        assertThat(headersCaptor.getValue()).doesNotContainKey(HEADER_DD_TRACE_ID);
    }

    @Test
    void shouldUseZeroFallbackWhenSpanIdIsNull() {
        Map<String, String> headers = createHeaders();

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(null);
            eventPublisher.publishToPubSub(List.of(TEST_TOPIC), createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        assertThat(headersCaptor.getValue()).containsEntry(HEADER_X_DATADOG_PARENT_ID, ZERO);
    }

    @Test
    void shouldNotInjectDdTraceIdWhenOnlyDdExists() {
        Map<String, String> headers = createHeaders();
        headers.put(HEADER_DD_TRACE_ID, PRE_EXISTING);

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishToPubSub(List.of(TEST_TOPIC), createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> captured = headersCaptor.getValue();
        assertThat(captured).containsEntry(HEADER_DD_TRACE_ID, PRE_EXISTING);
        assertThat(captured).containsEntry(HEADER_X_DATADOG_TRACE_ID, TEST_TRACE_ID);
    }

    @Test
    void shouldNotInjectXDatadogWhenOnlyXDatadogExists() {
        Map<String, String> headers = createHeaders();
        headers.put(HEADER_X_DATADOG_TRACE_ID, PRE_EXISTING);

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishToPubSub(List.of(TEST_TOPIC), createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(TEST_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        Map<String, String> captured = headersCaptor.getValue();
        assertThat(captured).containsEntry(HEADER_DD_TRACE_ID, TEST_TRACE_ID);
        assertThat(captured).containsEntry(HEADER_X_DATADOG_TRACE_ID, PRE_EXISTING);
        assertThat(captured).doesNotContainKey(HEADER_X_DATADOG_PARENT_ID);
    }

    // ==================== publishToPubSubWithOrderingKey ====================

    @Test
    void shouldPublishWithOrderingKey() {
        Map<String, String> headers = createHeadersWithAuth();
        String orderingKey = "order-123";

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishToPubSubWithOrderingKey(List.of(TEST_TOPIC), createData(), headers, orderingKey);
        }

        verify(pubSubOrderingKeyPublisherTemplate).publish(eq(TEST_TOPIC), pubsubMessageCaptor.capture());
        PubsubMessage captured = pubsubMessageCaptor.getValue();
        assertThat(captured.getOrderingKey()).isEqualTo(orderingKey);
        assertThat(captured.getData()).isEqualTo(ByteString.copyFromUtf8(TEST_VALUE));
        assertThat(captured.getAttributesMap()).containsEntry(HEADER_DD_TRACE_ID, TEST_TRACE_ID);
        assertThat(headers).doesNotContainKey(HEADER_AUTHORIZATION);
    }

    @Test
    void shouldRemoveNullHeaderValues() {
        Map<String, String> headers = new HashMap<>();
        headers.put("valid-header", DATA_VALUE);
        headers.put("null-header", null);

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishToPubSubWithOrderingKey(List.of(TEST_TOPIC), createData(), headers, "key-1");
        }

        verify(pubSubOrderingKeyPublisherTemplate).publish(eq(TEST_TOPIC), any(PubsubMessage.class));
        assertThat(headers).doesNotContainValue(null);
    }

    // ==================== publishToDLQ ====================

    @Test
    void shouldPublishToDlqTopic() {
        Map<String, String> headers = createHeadersWithAuth();

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishToDLQ(DLQ_TOPIC, createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(DLQ_TOPIC), eq(TEST_VALUE), any());
        assertThat(headers).doesNotContainKey(HEADER_AUTHORIZATION);
    }

    @Test
    void shouldInjectTraceContextInDlq() {
        Map<String, String> headers = createHeaders();

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishToDLQ(DLQ_TOPIC, createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq(DLQ_TOPIC), eq(TEST_VALUE), headersCaptor.capture());
        assertThat(headersCaptor.getValue()).containsEntry(HEADER_DD_TRACE_ID, TEST_TRACE_ID);
    }

    // ==================== publishReturnOrderToErrorTopic ====================

    @Test
    void shouldPublishToReturnRequestErrorTopic() {
        Map<String, String> headers = new HashMap<>();
        headers.put("valid-header", DATA_VALUE);
        headers.put("null-header", null);

        try (MockedStatic<CorrelationIdentifier> mocked = mockStatic(CorrelationIdentifier.class)) {
            mocked.when(CorrelationIdentifier::getTraceId).thenReturn(TEST_TRACE_ID);
            mocked.when(CorrelationIdentifier::getSpanId).thenReturn(TEST_SPAN_ID);
            eventPublisher.publishReturnOrderToErrorTopic(createData(), headers);
        }

        verify(pubSubPublisherTemplate).publish(eq("return-request-error"), eq(TEST_VALUE), any());
    }
}
