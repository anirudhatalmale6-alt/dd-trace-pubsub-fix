package osmos.commerce.sellerorder.publish;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PreDestroy;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;
import osmos.commerce.common.model.event.PubsubMessage;
import osmos.commerce.common.util.ToJsonConverter;
import osmos.commerce.sellerorder.service.status.EventHandlerStrategyDelegator;
import osmos.commerce.sellerorder.util.PubSubTraceContextHelper;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.split;
import static osmos.commerce.common.util.ApplicationConstants.ATTRIBUTE_EVENT_TYPE;
import static osmos.commerce.common.util.ApplicationConstants.ATTRIBUTE_SITE_ID;
import static osmos.commerce.common.util.ApplicationConstants.ATTRIBUTE_TENANT_ID;
import static osmos.commerce.common.util.ApplicationConstants.X_SITE_ID;
import static osmos.commerce.common.util.ApplicationConstants.X_TENANT_ID;
import static osmos.commerce.sellerorder.util.ApplicationConstants.ATTRIBUTE_X_PERF_ID;
import static osmos.commerce.sellerorder.util.ApplicationConstants.EVENT_MANIFEST_CREATED;
import static osmos.commerce.sellerorder.util.ApplicationConstants.IDEMPOTENT_KEY_FIELD_NAME;

@Slf4j
@Configuration
@RequiredArgsConstructor
@ConditionalOnProperty(
        value = "ENABLE_PULL_LISTENER_FOR_SO",
        havingValue = "true"
)
public class EventsPubSubPullService implements ApplicationListener<ApplicationReadyEvent> {

    @Value("${pubsub.pull-subscription-id}")
    private String subscriptionNames;

    @Value("${ENABLE_PULL_LISTENER_FOR_SO}")
    private boolean pullListenerEnabled;

    private final PubSubTemplate pubSubTemplate;
    private final EventHandlerStrategyDelegator eventHandlerStrategyDelegator;
    private final ToJsonConverter toJsonConverter;

    private static final int SHUTDOWN_TIMEOUT_SECONDS = 30;

    private final List<Subscriber> activeSubscribers = new ArrayList<>();

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        log.info("Streaming Pull Listener for Seller Order is enabled: {}", pullListenerEnabled);
        if (pullListenerEnabled) {
            MDC.clear();
            String[] subscriptions = subscriptionNames.contains("|") ?
                    split(subscriptionNames, "|") : split(subscriptionNames, ",");
            Set<String> subscriptionNames = Set.of(subscriptions);
            log.info("Subscriptions list :: {}", this.subscriptionNames);

            if (!CollectionUtils.isEmpty(subscriptionNames)) {
                subscriptionNames.forEach(this::startStreaming);
            }
        } else {
            log.info("Streaming Pull Listener for Seller Order is disabled");
        }
    }

    public void startStreaming(String subscription) {
        log.info("Starting streaming subscriber for subscription: {}", subscription);
        Subscriber subscriber = pubSubTemplate.subscribe(subscription,
                message -> processAndAcknowledgeMessage(message, subscription));
        activeSubscribers.add(subscriber);
        log.info("Streaming subscriber started for subscription: {}", subscription);
    }

    void processAndAcknowledgeMessage(BasicAcknowledgeablePubsubMessage message, String subscription) {
        try {
            MDC.clear();
            String payload = message.getPubsubMessage().getData().toStringUtf8();
            Map<String, String> attributes = new HashMap<>(message.getPubsubMessage().getAttributesMap());
            attributes.remove("googclient_deliveryattempt");
            PubsubMessage pubsubMessage = new PubsubMessage();

            pubsubMessage.setData(Base64.getEncoder().encodeToString(
                    payload.getBytes(StandardCharsets.UTF_8)));
            pubsubMessage.setMessageId(message.getPubsubMessage().getMessageId());
            pubsubMessage.setAttributes(attributes);

            osmos.commerce.sellerorder.model.PubsubRequest request = new osmos.commerce.sellerorder.model.PubsubRequest();
            request.setMessage(pubsubMessage);
            request.setSubscription(subscription);

            String eventType = attributes.get(ATTRIBUTE_EVENT_TYPE);

            log.info("Processing message from subscription :: {}", subscription);

            // Activate the propagated trace context from upstream service
            try (PubSubTraceContextHelper.TraceScope traceScope =
                         PubSubTraceContextHelper.activateTraceFromAttributes(
                                 attributes, "pubsub.pull." + subscription)) {
                routeMessage(request, eventType);
            }

            message.ack();
        } catch (Exception e) {
            log.error("Error processing message from subscription {}: ", subscription, e);
            message.nack();
        }
    }

    private void routeMessage(osmos.commerce.sellerorder.model.PubsubRequest pubsubRequest, String eventType) {
        PubsubMessage message = pubsubRequest.getMessage();
        log.info("Received event :: {} with data :: {} and header :: {}", eventType,
                message.getData(), toJsonConverter.convert(message.getAttributes()));

        Map<String, String> finalHeaders = determineHeaders(pubsubRequest);
        if (equalsIgnoreCase(pubsubRequest.getMessage().getEventType(), EVENT_MANIFEST_CREATED)) {
            eventHandlerStrategyDelegator.convertToCoreEventPayload(message);
        }
        eventHandlerStrategyDelegator.determineAndCallStrategy(pubsubRequest, finalHeaders);
    }

    private Map<String, String> determineHeaders(osmos.commerce.sellerorder.model.PubsubRequest pubsubRequest) {
        PubsubMessage message = pubsubRequest.getMessage();
        String tenantId = message.getAttributes().containsKey(ATTRIBUTE_TENANT_ID) ?
                String.valueOf(message.getTenantId()) : "";
        Map<String, String> headers = new HashMap<>(pubsubRequest.getMessage().getAttributes());
        headers.put(X_TENANT_ID, tenantId);
        if (message.getAttributes().containsKey(IDEMPOTENT_KEY_FIELD_NAME)) {
            headers.put(IDEMPOTENT_KEY_FIELD_NAME, message.getAttributes().get(IDEMPOTENT_KEY_FIELD_NAME));
        }
        if (message.getAttributes().containsKey(ATTRIBUTE_SITE_ID)) {
            headers.put(X_SITE_ID, message.getSiteId());
        }

        if (message.getAttributes().containsKey(ATTRIBUTE_X_PERF_ID)) {
            headers.put(ATTRIBUTE_X_PERF_ID, message.getAttributes().get(ATTRIBUTE_X_PERF_ID));
        }
        return headers;
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down {} streaming subscriber(s)...", activeSubscribers.size());
        activeSubscribers.forEach(subscriber -> {
            try {
                subscriber.stopAsync().awaitTerminated(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                log.info("Subscriber stopped successfully");
            } catch (TimeoutException e) {
                log.error("Subscriber did not terminate within {} seconds", SHUTDOWN_TIMEOUT_SECONDS, e);
            }
        });
        activeSubscribers.clear();
        log.info("All streaming subscribers shut down");
    }

    List<Subscriber> getActiveSubscribers() {
        return activeSubscribers;
    }
}
