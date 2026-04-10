package osmos.commerce.order.common.aspect;

import java.util.Map;

import javax.annotation.PostConstruct;

import datadog.trace.api.CorrelationIdentifier;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;

@Aspect
@Component
@Slf4j
public class PubSubPublishTraceAspect {

    @Before("execution(* *..PubSubPublisherTemplate.publish(..))")
    public void injectTraceId(JoinPoint joinPoint) {

        Object[] args = joinPoint.getArgs();

        if (args.length >= 3 && args[2] instanceof Map) {

            Map<String, String> attributes = (Map<String, String>) args[2];

            String traceId = CorrelationIdentifier.getTraceId();
            String spanId = CorrelationIdentifier.getSpanId();

            if (traceId != null && !traceId.equals("0") && !attributes.containsKey("x-datadog-trace-id")) {
                attributes.put("x-datadog-trace-id", traceId);
                attributes.put("x-datadog-parent-id", spanId != null ? spanId : "0");
                attributes.put("x-datadog-sampling-priority", "1");
            }

            log.info("PubSub publish intercepted");
            log.info("Injecting x-datadog-trace-id {} and x-datadog-parent-id {} into PubSub attributes", traceId, spanId);
            log.info("Attributes after aspect {}", attributes);
        }
    }

    @PostConstruct
    public void init() {
        log.info("PubSubPublishTraceAspect initialized");
    }
}
