package osmos.commerce.marketplace.orderorchestrator.common.aspect;

import com.google.cloud.spring.pubsub.support.converter.ConvertedAcknowledgeablePubsubMessage;
import io.opentracing.Scope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import osmos.commerce.marketplace.orderorchestrator.common.utils.PubSubTraceContextHelper;
import osmos.commerce.marketplace.orderorchestrator.common.utils.TenantMapper;
import osmos.commerce.marketplace.orderorchestrator.models.common.PubsubMessage;
import osmos.commerce.marketplace.orderorchestrator.models.common.PubsubRequest;

import java.util.Map;

import static osmos.commerce.marketplace.orderorchestrator.common.utils.ApplicationConstants.ATTRIBUTE_DELIVERY_ATTEMPT;
import static osmos.commerce.marketplace.orderorchestrator.common.utils.ApplicationConstants.ATTRIBUTE_EVENT_ID;
import static osmos.commerce.marketplace.orderorchestrator.common.utils.ApplicationConstants.ATTRIBUTE_EVENT_TYPE;

@Aspect
@Component
@RequiredArgsConstructor
@Slf4j
public class PubSubLogAspect implements MDCLogAspect {

    private final TenantMapper tenantMapper;

    // ThreadLocal to hold the trace scope for cleanup in @After
    private static final ThreadLocal<Scope> traceScope = new ThreadLocal<>();

    @Before("within(osmos.commerce.marketplace.orderorchestrator.controllers..*) and args (pubsubRequest)")
    public void aspect(JoinPoint joinPoint, PubsubRequest pubsubRequest) {
        request(pubsubRequest);
        message(pubsubRequest.getMessage());
        headers(pubsubRequest.getMessage().getAttributes());

        // Activate trace context from incoming PubSub message
        Scope scope = PubSubTraceContextHelper.activateTraceFromPubSub(pubsubRequest.getMessage().getAttributes());
        if (scope != null) {
            traceScope.set(scope);
        }
    }

    @After("within(osmos.commerce.marketplace.orderorchestrator.controllers..*) and args (pubsubRequest)")
    public void aspectClear(JoinPoint joinPoint, PubsubRequest pubsubRequest) {
        // Deactivate trace scope before clearing MDC
        PubSubTraceContextHelper.deactivateTrace(traceScope.get());
        traceScope.remove();
        MDC.clear();
    }


    @Before("within(osmos.commerce.marketplace.orderorchestrator.publish..*) and args (message, subscription)")
    public void aspectPull(JoinPoint joinPoint, ConvertedAcknowledgeablePubsubMessage<String> message, String subscription) {
        headers();
        message(message);
        Map<String, String> attributes = message.getPubsubMessage().getAttributesMap();
        headers(attributes);

        // Activate trace context from incoming PubSub message
        Scope scope = PubSubTraceContextHelper.activateTraceFromPubSub(attributes);
        if (scope != null) {
            traceScope.set(scope);
        }
    }

    @After("within(osmos.commerce.marketplace.orderorchestrator.publish..*) and args (message, subscription)")
    public void aspectClear(JoinPoint joinPoint, ConvertedAcknowledgeablePubsubMessage<String> message, String subscription) {
        // Deactivate trace scope before clearing MDC
        PubSubTraceContextHelper.deactivateTrace(traceScope.get());
        traceScope.remove();
        MDC.clear();
    }

    private void request(PubsubRequest request) {
        MDC.put(ATTRIBUTE_DELIVERY_ATTEMPT, request.getDeliveryAttempt());
    }


    private void message(PubsubMessage message) {
        MDC.put(ATTRIBUTE_EVENT_ID, message.getEventId());
        MDC.put(ATTRIBUTE_EVENT_TYPE, message.getEventType());
    }

    private void message(ConvertedAcknowledgeablePubsubMessage message) {
        MDC.put(ATTRIBUTE_EVENT_ID, message.getPubsubMessage().getAttributesOrDefault(ATTRIBUTE_EVENT_ID, ""));
        MDC.put(ATTRIBUTE_EVENT_TYPE, message.getPubsubMessage().getAttributesOrDefault(ATTRIBUTE_EVENT_TYPE, ""));
    }

    @Override
    public TenantMapper getTenantMapper() {
        return tenantMapper;
    }
}
