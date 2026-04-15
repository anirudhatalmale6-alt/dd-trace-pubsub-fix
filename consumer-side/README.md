# Consumer-Side Trace Propagation

## Problem
The publishing side (PubSubPublishTraceAspect + EventPublisher.injectTraceContext) correctly injects
x-datadog-trace-id and x-datadog-parent-id into PubSub message attributes. However, consuming
artifacts start NEW traces instead of continuing the propagated one.

## Solution
Add consumer-side trace extraction that reads the Datadog headers from incoming PubSub messages
and activates a child span, continuing the same trace.

## Files per artifact

### Artifact B (seller-orders)

1. ADD new file: `src/main/java/osmos/commerce/sellerorder/util/PubSubTraceContextHelper.java`
   - Utility class to extract trace context and create child spans

2. REPLACE: `src/main/java/osmos/commerce/sellerorder/controller/OrderingEventsController.java`
   - Wraps message processing in try-with-resources TraceScope block

3. REPLACE: `src/main/java/osmos/commerce/sellerorder/controller/EventsController.java`
   - Wraps message processing in try-with-resources TraceScope block

4. REPLACE: `src/main/java/osmos/commerce/sellerorder/publish/EventsPubSubPullService.java`
   - Wraps processAndAcknowledgeMessage in try-with-resources TraceScope block

5. ADD to build.gradle dependencies:
   ```
   compileOnly 'io.opentracing:opentracing-api:0.33.0'
   compileOnly 'io.opentracing:opentracing-util:0.33.0'
   ```

### Artifact C (marketplace-order-orchestrator)

1. ADD new file: `src/main/java/osmos/commerce/marketplace/orderorchestrator/common/utils/PubSubTraceContextHelper.java`
   - Utility class to extract trace context and create child spans

2. REPLACE: `src/main/java/osmos/commerce/marketplace/orderorchestrator/common/aspect/PubSubLogAspect.java`
   - Added trace activation in @Before and deactivation in @After for both push and pull paths

3. ADD to build.gradle dependencies:
   ```
   compileOnly 'io.opentracing:opentracing-api:0.33.0'
   compileOnly 'io.opentracing:opentracing-util:0.33.0'
   ```

## Dependency Note
The OpenTracing API is declared as `compileOnly` because at runtime, the Datadog Java Agent
(dd-java-agent.jar) already bundles the OpenTracing implementation. We only need it for compilation.

## Expected behavior after deployment
- Artifact A publishes event with x-datadog-trace-id=TRACE_A
- Artifact B receives it, extracts TRACE_A, creates child span -> dd.trace_id matches TRACE_A
- Artifact B publishes downstream event, PubSubPublishTraceAspect injects the SAME trace ID
- Artifact C receives it, extracts the trace, creates child span -> dd.trace_id matches TRACE_A
- All 3 artifacts share the same trace ID in Datadog APM

## Verification
After deploying, search logs for:
- "Activated propagated trace context: parent_trace_id=X, new dd.trace_id=X"
  (new dd.trace_id should match or be a child of parent_trace_id)
- The dd.trace_id in artifact B and C logs should match artifact A's dd.trace_id
