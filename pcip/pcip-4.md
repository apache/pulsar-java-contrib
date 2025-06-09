# PCIP-4: Improve delayed RPC message handling in pulsar-rpc

# Background knowledge

In the Pulsar RPC framework, the client typically places an RPC request into a `pendingRequestsMap`. When the server processes this request, it sends back a reply, which in turn matches the original request in `pendingRequestsMap`. If there is no corresponding pending request, the reply is “discarded” and no result is returned to the caller.

However, when an RPC request is delayed (e.g., not delivered until a scheduled time in the future), the original design would require leaving the request in the `pendingRequestsMap` for an extended period. This can cause potential resource and management overhead, especially when the delay is long.

# Motivation

We want to enable the **delayed sending** of RPC requests, where:

- The request can be sent immediately, but only delivered to the server at or after a certain timestamp
- The request can be sent to the server only after a specified relative delay.

In such cases, holding the request in the `pendingRequestsMap` for a long time (until the server receives and processes the message) is suboptimal and leads to potential inefficiencies or timeouts. Therefore, we introduce a change in behavior to avoid adding these delayed requests to the `pendingRequestsMap` immediately on the client side.

When the delayed request finally arrives at the server and the server sends a reply, the reply still needs to be matched to the original request ID. Hence, we create a mechanism so that, upon receiving the delayed reply on the client side, the client reconstructs the pending request context on demand (i.e., adding it into the `pendingRequestsMap` just in time) and completes the callback flow.

# Goals

## In Scope

- Provide APIs to schedule an RPC request at a specified absolute time (`requestAtAsync`) or after a specified relative delay (`requestAfterAsync`).
- Adjust the Pulsar RPC client flow so that **delayed** requests are **not** tracked in `pendingRequestsMap` immediately, thereby reducing resource usage.
- Ensure the **reply** for a delayed request can still be correlated, even if it arrives much later.

## Out of Scope

- Enhancements to Pulsar’s core delayed-delivery mechanism. The changes here rely on Pulsar’s existing delayed-delivery support.
- Detailed transaction or rollback logic on the client side for delayed requests. That remains part of user-defined logic.

# High Level Design

When the user calls:
- `requestAtAsync(correlationId, value, timestamp)` or
- `requestAfterAsync(correlationId, value, delay, TimeUnit unit)`

the client **immediately** sends a message to the request topic with additional properties indicating delayed delivery (`deliverAt(...)` or `deliverAfter(...)`). However, it **does not** store the request in the `pendingRequestsMap`. Instead, it simply sends the message to Pulsar with the appropriate delay/timestamp.

When the server eventually processes the message and responds, the client’s `ReplyListener` checks if this reply corresponds to a **delayed** request by looking at specific properties (e.g. `REQUEST_DELIVER_AT_TIME`). If so, it **lazily** puts the request into the `pendingRequestsMap`, completes any callback logic, and removes the request.

# Detailed Design

## Design & Implementation Details

Below is an overview of the **key changes** from the code:

1. **New Methods in `PulsarRpcClient`:**

- requestAtAsync(String correlationId, T value, long timestamp)
- requestAfterAsync(String correlationId, T value, long delay, TimeUnit unit)
- And their overloads that accept Map<String, Object> config.

These methods allow the client to specify absolute (requestAtAsync) or relative (requestAfterAsync) delivery times.

2. **Implementation in `PulsarRpcClientImpl`:**

```java
    @Override
    public CompletableFuture<V> requestAtAsync(String correlationId, T value, Map<String, Object> config,
                                               long timestamp) {
        return internalRequest(correlationId, value, config, timestamp, -1, null);
    }

    @Override
    public CompletableFuture<V> requestAfterAsync(String correlationId, T value, Map<String, Object> config,
                                                  long delay, TimeUnit unit) {
        return internalRequest(correlationId, value, config, -1, delay, unit);
    }

    private CompletableFuture<V> internalRequest(String correlationId, T value, Map<String, Object> config,
                                                 long timestamp, long delay, TimeUnit unit) {
        CompletableFuture<V> replyFuture = new CompletableFuture<>();
        // ...
        if (timestamp == -1 && delay == -1) {
            // Normal, non-delayed behavior: add request to pendingRequestsMap and handle as before.
        } else {
            // Delayed behavior: do NOT immediately add to pendingRequestsMap.
            // Instead, set deliverAt or deliverAfter properties. Send message. 
            // The actual correlation is done when a reply arrives.
        }
        return replyFuture;
    }
```

- If not delayed (both timestamp == -1 and delay == -1), the client uses the old path of tracking in pendingRequestsMap.
- If the request is delayed, it sets special properties (e.g. REQUEST_DELIVER_AT_TIME) and does not add the request to pendingRequestsMap.

3.	Server-Side and Reply Handling:

- A new property REQUEST_DELIVER_AT_TIME is used to record the scheduled delivery time.
- If the server sees a request containing REQUEST_DELIVER_AT_TIME, it processes it after the actual delivery time is reached (relying on the broker’s delayed-delivery feature).
- When the server responds, it includes the same property for the client’s reference.
- On the client side, ReplyListener checks if REQUEST_DELIVER_AT_TIME is present:

```java
    if (!pendingRequestsMap.containsKey(correlationId) && !msg.hasProperty(REQUEST_DELIVER_AT_TIME)) {
        // This is a normal message with no matching pending request
        ...
    } else {
        // If it’s a delayed message, we computeIfAbsent in the map and proceed with callback logic
    }
```

Delayed replies are inserted into the map, and the user’s callback (`RequestCallBack.onReplySuccess`, etc.) is triggered.

## Public-facing Changes

### Public API

New methods in PulsarRpcClient:

```java
    /**
     * Deliver the message only at or after the specified absolute timestamp.
     * Asynchronously sends a request and returns a future that completes with the reply.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @param timestamp Absolute timestamp indicating when the message should be delivered to rpc-server.
     * @return A CompletableFuture that will complete with the reply value.
     */
    default CompletableFuture<V> requestAtAsync(String correlationId, T value, long timestamp) {
        return requestAtAsync(correlationId, value, Collections.emptyMap(), timestamp);
    }

    /**
     * Deliver the message only at or after the specified absolute timestamp.
     * Asynchronously sends a request and returns a future that completes with the reply.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @param config Configuration map for creating a request producer,
     *              will call {@link TypedMessageBuilder#loadConf(Map)}
     * @param timestamp Absolute timestamp indicating when the message should be delivered to rpc-server.
     * @return A CompletableFuture that will complete with the reply value.
     */
    CompletableFuture<V> requestAtAsync(String correlationId, T value, Map<String, Object> config,
                                        long timestamp);

    /**
     * Request to deliver the message only after the specified relative delay.
     * Asynchronously sends a request and returns a future that completes with the reply.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @param delay The amount of delay before the message will be delivered.
     * @param unit The time unit for the delay.
     * @return A CompletableFuture that will complete with the reply value.
     */
    default CompletableFuture<V> requestAfterAsync(String correlationId, T value, long delay, TimeUnit unit) {
        return requestAfterAsync(correlationId, value, Collections.emptyMap(), delay, unit);
    }

    /**
     * Request to deliver the message only after the specified relative delay.
     * Asynchronously sends a request and returns a future that completes with the reply.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @param config Configuration map for creating a request producer,
     *              will call {@link TypedMessageBuilder#loadConf(Map)}
     * @param delay The amount of delay before the message will be delivered.
     * @param unit The time unit for the delay.
     * @return A CompletableFuture that will complete with the reply value.
     */
    CompletableFuture<V> requestAfterAsync(String correlationId, T value, Map<String, Object> config,
                                                  long delay, TimeUnit unit);
```

### Configuration

### CLI

# Get started

## Quick Start

Below is a practical example of delayed RPC using requestAfterAsync, referencing a test scenario similar to testDelayedRpcAt from our SimpleRpcCallTest.

1. Create a PulsarClient, define schemas, and initialize topics:

```java
    PulsarClient pulsarClient = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .build();

    Schema<TestRequest> requestSchema = Schema.JSON(TestRequest.class);
    Schema<TestReply> replySchema = Schema.JSON(TestReply.class);

    String requestTopic = "testDelayedRpcAt-request";
    String replySubscription = "testDelayedRpcAt-reply-sub";

    final int ackNums = 2;      // For demonstration: we want to remove the request after we see replies multiple times.
    final int messageNum = 5;   // Example count of messages to send
    final long delayedTime = 5000; // 5 seconds delay
```

2. Implement a RequestCallBack to handle events like “send success”, “reply success/error”, and “timeout”:

```java
// Example records:
public record TestRequest(String value) { }
public record TestReply(String value) { }

Map<String, AtomicInteger> resultMap = new ConcurrentHashMap<>();

// This callback will track how many times each correlationId sees a reply.
RequestCallBack<TestReply> callBack = new RequestCallBack<>() {
    @Override
    public void onSendRequestSuccess(String correlationId, MessageId messageId) {
        log.info("<onSendRequestSuccess> CorrelationId[{}] Send request message success. MessageId: {}",
                correlationId, messageId);
    }

    @Override
    public void onSendRequestError(String correlationId, Throwable t,
                                   CompletableFuture<TestReply> replyFuture) {
        log.warn("<onSendRequestError> CorrelationId[{}] failed. {}",
                correlationId, t.getMessage());
        replyFuture.completeExceptionally(t);
    }

    @Override
    public void onReplySuccess(String correlationId, String subscription,
                               TestReply value, CompletableFuture<TestReply> replyFuture) {
        log.info("<onReplySuccess> CorrelationId[{}] Subscription[{}] Value: {}",
                correlationId, subscription, value);
        // Count the number of times we've successfully received a reply for this correlationId.
        if (resultMap.get(correlationId).getAndIncrement() == ackNums - 1) {
            // Once we hit ackNums, remove the request from the pending map
            rpcClient.removeRequest(correlationId);
        }
        replyFuture.complete(value);
    }

    @Override
    public void onReplyError(String correlationId, String subscription,
                             String errorMessage, CompletableFuture<TestReply> replyFuture) {
        log.warn("<onReplyError> CorrelationId[{}] Subscription[{}] Error: {}",
                correlationId, subscription, errorMessage);
        replyFuture.completeExceptionally(new Exception(errorMessage));
    }

    @Override
    public void onTimeout(String correlationId, Throwable t) {
        log.warn("<onTimeout> CorrelationId[{}] Timed out. {}", correlationId, t.getMessage());
    }

    @Override
    public void onReplyMessageAckFailed(String correlationId, Consumer<TestReply> consumer,
                                        Message<TestReply> msg, Throwable t) {
        consumer.acknowledgeAsync(msg.getMessageId()).exceptionally(ex -> {
            log.warn("<onReplyMessageAckFailed> Acknowledging message {} failed again.", msg.getMessageId(), ex);
            return null;
        });
    }
};
```

3. Create the RPC Client using requestCallBack(callBack):

```java
Map<String, Object> requestProducerConfigMap = new HashMap<>();
requestProducerConfigMap.put("producerName", "requestProducer");
requestProducerConfigMap.put("messageRoutingMode", MessageRoutingMode.RoundRobinPartition);

// Build the RPC client
PulsarRpcClient<TestRequest, TestReply> rpcClient =
        PulsarRpcClient.builder(requestSchema, replySchema)
                .requestTopic(requestTopic)
                .requestProducerConfig(requestProducerConfigMap)
                .replySubscription(replySubscription)
                .replyTimeout(Duration.ofSeconds(10))
                .requestCallBack(callBack)    // Set the callback
                .build(pulsarClient);
```

4. Create an RPC Server (or multiple servers) that subscribes to the request topic, processes incoming messages, and sends replies:

```java
// Example of request processing logic:
AtomicInteger epoch = new AtomicInteger(1);

Function<TestRequest, CompletableFuture<TestReply>> requestFunction = request -> {
    epoch.getAndIncrement();
    return CompletableFuture.completedFuture(new TestReply(request.value() + "-----------done"));
};

BiConsumer<String, TestRequest> rollbackFunction = (id, request) -> {
    // Example rollback logic
    epoch.set(1);
};

// Create 1 or more RPC servers listening on the same subscription but different consumer names
PulsarRpcServer<TestRequest, TestReply> rpcServer = PulsarRpcServer
        .builder(requestSchema, replySchema)
        .requestTopic(requestTopic)
        .requestSubscription("myServerSub-1")
        .build(pulsarClient, requestFunction, rollbackFunction);
```

5. Send delayed requests with requestAfterAsync (relative delay). For each request, we use a unique correlationId and track its replies in resultMap:

```java
    // For each message, we store an AtomicInteger to track how many replies we've received for that correlationId
    Map<String, Object> requestMessageConfigMap = new HashMap<>();
    requestMessageConfigMap.put(TypedMessageBuilder.CONF_DISABLE_REPLICATION, true);

    for (int i = 0; i < messageNum; i++) {
        String correlationId = "corr-" + i;
        TestRequest message = new TestRequest("DelayedRPC-" + i);
        resultMap.put(correlationId, new AtomicInteger());
        rpcClient.requestAfterAsync(correlationId, message, requestMessageConfigMap,
                                    delayedTime, TimeUnit.MILLISECONDS);
    }
```

This sends each request immediately to Pulsar but tells the broker to delay actual delivery to the server by 5 seconds.

6. Verify results after waiting enough time (e.g., using Awaitility in tests):

```java
    long current = System.currentTimeMillis();

    Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> {
        AtomicInteger success = new AtomicInteger();
        resultMap.forEach((__, count) -> success.getAndAdd(count.get()));
        if (System.currentTimeMillis() - current < delayedTime && success.get() > 0) {
            return false;
        }
        return success.get() >= messageNum * ackNums && System.currentTimeMillis() - current >= delayedTime;
    });

    // Close servers if needed
    rpcServer.close();
```

7. Confirm that each request’s callback logic (onReplySuccess, onTimeout, etc.) behaves as expected. Once ackNums replies have been processed (or you decide your “success” threshold), the RPC client can call removeRequest(correlationId) to stop tracking it.

This flow demonstrates how to schedule or delay Pulsar RPC calls without holding them in memory for the entire delay period, yet still receive normal request–reply handling after the delay expires.
