package osmos.commerce.sellerorder.controller;

import java.util.List;
import java.util.Map;

import javax.validation.Valid;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import osmos.commerce.common.model.event.PubsubMessage;
import osmos.commerce.common.model.event.core.CoreEvent;
import osmos.commerce.sellerorder.auth.Country;
import osmos.commerce.sellerorder.auth.TenantMapper;
import osmos.commerce.sellerorder.model.PubsubRequest;
import osmos.commerce.sellerorder.publish.EventPublisher;
import osmos.commerce.sellerorder.service.status.EventMapper;
import osmos.commerce.sellerorder.util.PubSubTraceContextHelper;
import osmos.commerce.sellerorder.util.PubsubAttributesEnricher;

import static java.lang.String.format;
import static java.lang.String.valueOf;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static org.springframework.http.HttpStatus.OK;
import static osmos.commerce.common.util.ApplicationConstants.AUTHORIZATION;
import static osmos.commerce.common.util.ApplicationConstants.EVENT_ORDER_CREATED;
import static osmos.commerce.sellerorder.util.ApplicationConstants.ATTRIBUTE_COUNTRY;
import static osmos.commerce.sellerorder.util.ApplicationConstants.EVENT_CUSTOMER_ORDER_CANCELLED_FOR_SELLER_ORDER;
import static osmos.commerce.sellerorder.util.ObjectMapperUtils.writeValueAsString;

@RestController
@RequestMapping("/v1/ordering-events")
@RequiredArgsConstructor
@Slf4j
public class OrderingEventsController {
    public static final String DATA = "data";
    public static final String ORDER_NUMBER = "orderNumber";
    private final PubsubAttributesEnricher attributesEnricher;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final EventMapper eventMapper;
    private final TenantMapper tenantMapper;

    @Value("${events.seller-orders.core.ordering.topic.name}")
    private String coreSellerOrderOrderingEventsTopicName;

    public static final String JSON_PATH = "$..";

    @PostMapping
    @ResponseStatus(OK)
    @SneakyThrows
    public ResponseEntity<?> receiveOrderingEvents(@RequestHeader(name = AUTHORIZATION) String authorization,
                                                   @RequestAttribute Map<String, String> tracingHeaders,
                                                   @Valid @RequestBody PubsubRequest pubsubRequest) {
        PubsubMessage message = attributesEnricher.enrichWithSubscription(pubsubRequest).getMessage();

        log.info("before tracingheaders: {}", tracingHeaders);
        message.getAttributes().forEach((k, v) -> {
            if (v != null) {
                tracingHeaders.putIfAbsent(k, v);
            }
        });
        log.info("after tracingheaders: {}", tracingHeaders);

        String eventType = message.getEventType();

        // Activate the propagated trace context from Artifact A
        // This tells the dd-java-agent to continue the original trace
        // instead of using its own auto-generated one
        try (PubSubTraceContextHelper.TraceScope traceScope =
                     PubSubTraceContextHelper.activateTraceFromAttributes(
                             message.getAttributes(), "ordering-events.process")) {

            log.info(escapeJava(format("Received ordering-events PubSub Event  :: %s, from subscription :: %s",
                    writeValueAsString(message),
                    pubsubRequest.getSubscription())));

            CoreEvent coreEvent = eventMapper.determineEvent(message, eventType);

            String orderNumber = getOrderNumber(message, eventType);

            String countryName = getCountryNameFromTenantId(valueOf(coreEvent.getTenantId()));
            tracingHeaders.put(ATTRIBUTE_COUNTRY, countryName);

            eventPublisher.publishEventWithOrderingKey(List.of(coreSellerOrderOrderingEventsTopicName), Map.of("data", coreEvent.getData()),
                    message.getAttributes(), orderNumber);
        }

        return ResponseEntity.status(OK).build();
    }


    private String getOrderNumber(PubsubMessage message, String eventType) throws JsonProcessingException {

        String orderNumber = "0000000000";
        JsonNode orderNumberJsonNode = null;

        if (equalsIgnoreCase(eventType, EVENT_ORDER_CREATED) || equalsIgnoreCase(eventType, EVENT_CUSTOMER_ORDER_CANCELLED_FOR_SELLER_ORDER)) {
            orderNumberJsonNode = objectMapper.readTree(message.unwrap()).get(DATA).get("order").get(ORDER_NUMBER);
        }

        if (equalsIgnoreCase(eventType, "reservationConfirmed")) {

            /* in case of Async flow */
            JsonNode jsonNode = objectMapper.readTree(message.unwrap()).get(DATA);
            if (jsonNode != null && jsonNode.get(DATA) != null) {
                orderNumberJsonNode = objectMapper.readTree(message.unwrap()).get(DATA).get(DATA).get(ORDER_NUMBER);
            }
            /* in case of  Sync flow */
            if (orderNumberJsonNode == null) {
                orderNumberJsonNode = objectMapper.readTree(message.unwrap()).get(DATA).get("response").get(ORDER_NUMBER);
            }
            if (orderNumberJsonNode == null) {
                orderNumberJsonNode = objectMapper.readTree(message.unwrap()).get(DATA).get("request").get(DATA).get(ORDER_NUMBER);
            }

        }

        if (equalsIgnoreCase(eventType, "paymentStatusUpdate")) {
            orderNumberJsonNode = objectMapper.readTree(message.unwrap()).get(DATA).get("paymentIntent").get(ORDER_NUMBER);
        }

        if (orderNumberJsonNode != null) {
            return orderNumberJsonNode.asText();
        }
        log.info("Couldn't find order number so passing default ordering key :: {}", orderNumber);
        return orderNumber;
    }

    private String getCountryNameFromTenantId(String tenantId) {
        String operatorCode = tenantMapper.getOperatorCode(tenantId);
        log.info(escapeJava(format("Getting operatorCode :: %s from tenantId :: %s", operatorCode, tenantId)));
        return Country.getCountryCodeByOperatorCode(operatorCode);
    }

}
