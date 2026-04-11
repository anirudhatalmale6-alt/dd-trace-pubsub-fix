package osmos.commerce.sellerorder.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.validation.Valid;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import osmos.commerce.common.model.event.PubsubMessage;
import osmos.commerce.sellerorder.model.PubsubRequest;
import osmos.commerce.sellerorder.service.status.EventHandlerStrategyDelegator;
import osmos.commerce.sellerorder.util.EnvironmentProperties;
import osmos.commerce.sellerorder.util.MDCUtil;
import osmos.commerce.sellerorder.util.PubSubTraceContextHelper;
import osmos.commerce.sellerorder.util.PubsubAttributesEnricher;
import osmos.commerce.sellerorder.validator.EventsValidator;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static org.springframework.http.HttpStatus.OK;
import static osmos.commerce.common.util.ApplicationConstants.ATTRIBUTE_SITE_ID;
import static osmos.commerce.common.util.ApplicationConstants.ATTRIBUTE_TENANT_ID;
import static osmos.commerce.common.util.ApplicationConstants.AUTHORIZATION;
import static osmos.commerce.common.util.ApplicationConstants.X_SITE_ID;
import static osmos.commerce.common.util.ApplicationConstants.X_TENANT_ID;
import static osmos.commerce.sellerorder.util.ApplicationConstants.ATTRIBUTE_X_PERF_ID;
import static osmos.commerce.sellerorder.util.ApplicationConstants.EVENT_MANIFEST_CREATED;
import static osmos.commerce.sellerorder.util.ApplicationConstants.EVENT_ORDER_STATUS_CREATED;
import static osmos.commerce.sellerorder.util.ApplicationConstants.IDEMPOTENT_KEY_FIELD_NAME;
import static osmos.commerce.sellerorder.util.ObjectMapperUtils.writeValueAsString;

@RestController
@RequestMapping("/v1/events")
@RequiredArgsConstructor
@Slf4j
public class EventsController {
    private final EventHandlerStrategyDelegator eventHandlerStrategyDelegator;
    private final EventsValidator eventsValidator;
    private final EnvironmentProperties environmentProperties;
    private final PubsubAttributesEnricher attributesEnricher;

    @PostMapping(path = {"", "/seller-shipment", "/customer-cancel", "/shipments"})
    @ResponseStatus(OK)
    @SneakyThrows
    public ResponseEntity<?> receiveEvents(@RequestHeader(name = AUTHORIZATION) String authorization,
                                           @RequestAttribute Map<String, String> tracingHeaders,
                                           @Valid @RequestBody PubsubRequest pubsubRequest) {
        PubsubMessage message = attributesEnricher.enrichWithSubscription(pubsubRequest).getMessage();
        String tenantId = message.getAttributes().containsKey(ATTRIBUTE_TENANT_ID) ?
                String.valueOf(message.getTenantId()) : "";

        String eventType = message.getEventType();

        String version = message.getAttributes().getOrDefault("version", "");

        MDCUtil.setMDCAttributes(pubsubRequest);

        if (!environmentProperties.getAllowedTenantIds().contains(tenantId)) {
            log.info(escapeJava(format("Unsupported tenantId :: %s for event :: %s", tenantId, eventType)));
            return ResponseEntity.status(OK).build();
        }

        if (!eventsValidator.isValidEvent(message)) {
            log.info(escapeJava(format("Ignoring the event :: %s ", eventType)));
            return ResponseEntity.status(OK).build();
        }

        String deliveryAttemptLog = Optional.ofNullable(pubsubRequest.getDeliveryAttempt())
                .map(s -> format(" and delivery attempt :: %s", s))
                .orElse("");
        log.info(escapeJava(format("Received PubSub Event :: %s, from subscription :: %s%s",
                writeValueAsString(message),
                pubsubRequest.getSubscription(),
                deliveryAttemptLog)));

        if (equalsIgnoreCase(EVENT_ORDER_STATUS_CREATED, eventType) &&
                !equalsIgnoreCase(environmentProperties.getAllowedOrderCreatedVersion(), version)) {
            log.info(escapeJava(format("Ignoring the event :: %s with version :: %s", eventType, version)));
            return ResponseEntity.status(OK).build();
        }

        Map<String, String> headers = new HashMap<>(tracingHeaders);
        headers.put(AUTHORIZATION, authorization);
        headers.put(X_TENANT_ID, tenantId);

        if (message.getAttributes().containsKey(ATTRIBUTE_SITE_ID)) {
            headers.put(X_SITE_ID, message.getSiteId());
        }

        if (message.getAttributes().containsKey(ATTRIBUTE_X_PERF_ID)) {
            headers.put(ATTRIBUTE_X_PERF_ID, message.getAttributes().get(ATTRIBUTE_X_PERF_ID));
        }

        if (message.getAttributes().containsKey(IDEMPOTENT_KEY_FIELD_NAME)) {
            headers.put(IDEMPOTENT_KEY_FIELD_NAME, message.getAttributes().get(IDEMPOTENT_KEY_FIELD_NAME));
        }

        if (equalsIgnoreCase(pubsubRequest.getMessage().getEventType(), EVENT_MANIFEST_CREATED)) {
            eventHandlerStrategyDelegator.convertToCoreEventPayload(message);
        }

        // Activate the propagated trace context from the upstream service
        // This tells the dd-java-agent to continue the original trace
        // instead of using its own auto-generated one
        try (PubSubTraceContextHelper.TraceScope traceScope =
                     PubSubTraceContextHelper.activateTraceFromAttributes(
                             message.getAttributes(), "events.process")) {
            eventHandlerStrategyDelegator.determineAndCallStrategy(pubsubRequest, headers);
        }

        return ResponseEntity.status(OK).build();

    }


}
