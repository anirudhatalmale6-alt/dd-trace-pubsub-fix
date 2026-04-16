package osmos.commerce.marketplace.orderorchestrator.common.aspect;

import com.google.cloud.spring.pubsub.support.converter.ConvertedAcknowledgeablePubsubMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;
import osmos.commerce.marketplace.orderorchestrator.common.utils.PubSubTraceContextHelper;
import osmos.commerce.marketplace.orderorchestrator.common.utils.TenantMapper;
import osmos.commerce.marketplace.orderorchestrator.models.common.PubsubMessage;
import osmos.commerce.marketplace.orderorchestrator.models.common.PubsubRequest;

import static osmos.commerce.marketplace.orderorchestrator.common.utils.ApplicationConstants.ATTRIBUTE_DELIVERY_ATTEMPT;
import static osmos.commerce.marketplace.orderorchestrator.common.utils.ApplicationConstants.ATTRIBUTE_EVENT_ID;
import static osmos.commerce.marketplace.orderorchestrator.common.utils.ApplicationConstants.ATTRIBUTE_EVENT_TYPE;

@Aspect
@Component
@RequiredArgsConstructor
@Slf4j
public class PubSubLogAspect implements MDCLogAspect {

    private final TenantMapper tenantMapper;

    @Around("within(osmos.commerce.marketplace.orderorchestrator.controllers..*) && args(pubsubRequest)")
    public Object aroundPushController(ProceedingJoinPoint joinPoint, PubsubRequest pubsubRequest) throws Exception {
        request(pubsubRequest);
        message(pubsubRequest.getMessage());
        headers(pubsubRequest.getMessage().getAttributes());

        try {
            return PubSubTraceContextHelper.executeWithTrace(
                    pubsubRequest.getMessage().getAttributes(), "pubsub.push.process",
                    () -> {
                        try {
                            return joinPoint.proceed();
                        } catch (Exception e) {
                            throw e;
                        } catch (Throwable t) {
                            throw new RuntimeException(t);
                        }
                    });
        } finally {
            MDC.clear();
        }
    }

    @Around("within(osmos.commerce.marketplace.orderorchestrator.publish..*) && args(message, subscription)")
    public Object aroundPullSubscriber(ProceedingJoinPoint joinPoint,
                                        ConvertedAcknowledgeablePubsubMessage<String> message,
                                        String subscription) throws Exception {
        headers();
        message(message);
        headers(message.getPubsubMessage().getAttributesMap());

        try {
            return PubSubTraceContextHelper.executeWithTrace(
                    message.getPubsubMessage().getAttributesMap(), "pubsub.pull.process",
                    () -> {
                        try {
                            return joinPoint.proceed();
                        } catch (Exception e) {
                            throw e;
                        } catch (Throwable t) {
                            throw new RuntimeException(t);
                        }
                    });
        } finally {
            MDC.clear();
        }
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
