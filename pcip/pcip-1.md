# PCIP-1: Distributed RPC framework implemented by the Pulsar client

# Background knowledge

## Request-Reply Synchronize Model

In this model, the client sends a request and waits for a response from the server. The server receives the request, processes it, and sends back a response. This interaction pattern is fundamental in client-server communication and is crucial for synchronous operations where the client needs a response before proceeding.

## Remote Procedure Call (RPC)

RPC allows functions or procedures to be executed on a different machine from the client making the call, as if they were local. This method abstracts the complexities of network communication, allowing developers to focus on the business logic rather than the underlying network details.

The implementation of RPC is usually based on the Request-Reply model. In this case:

- The RPC client plays the role of the requester, calling a remote procedure as if it were sending a request message.
- The RPC server plays the role of the responder, receiving the request, executing the procedure, and returning the result as a reply message.

## Current behavior of sending messages in Pulsar

The current sending behavior of Pulsar is when the message is successfully published, that is, successfully persisted to the storage layer. The MessageId assigned to the published message by the broker is returned.

## Analogies of message flow in RPC and Pulsar

- In Pulsar, producer is equivalent to RPC Client.
- The RPC Client initiates a request like sending a message producer.
- The RPC Server side receives this request as if the consumer receives the message and then carries out customized processing, and finally ACKs the message.
- If this ACK request contains the result returned by the "server side" and is sent to the original producer(RPC Client).
- After receiving the results returned by the consumer(RPC Server), the producer(RPC Client) directly returns the content of the results.

# Motivation

As we known，Pulsar's current asynchronous publish-subscribe model serves well for decoupled message distribution, but it lacks a native mechanism for handling synchronous interactions typical of Remote Procedure Calls (RPC).
This request-reply model can greatly enhance the utility of Pulsar. We can then use Pulsar as a distributed RPC Framework. This also improves the fault tolerance of RPC calls.

PIP-371 is also committed to building such a model.

(https://github.com/apache/pulsar/pull/23143 and https://github.com/apache/pulsar/pull/23194)

But we need to implement this distributed RPC framework in a way that does not intrude into the pulsar core library. 
Therefore, we need to use two topics, one is the request topic and the other is the reply topic. The client side sends RPC requests to the request topic, the server side receives request message and performs customized processing, and finally sends them to the reply topic. The client receives the reply message and returns.

### Why would we use Pulsar for this RPC call?

Implement RPC using Apache Pulsar. Requests can be sent through a client, received by one or more servers and processed in parallel. Finally, the server returns all processing results after processing, and the client can perform summary and other operations after receiving them.
This proposal to achieve the function is request. Request and existing send function of pulsar can be mixed to same topic. This means that the user can choose, and the call to the server side (consumer) can be asynchronous or synchronous, which is controlled by the user flexibly.
You can directly use Pulsar's own delaying messages, that is, you can execute RPC regularly.
You can directly use Pulsar's own load balancing mechanism.
You can directly use Pulsar's own message consumption throttling mechanism.
You can directly use Pulsar's own expansion and contraction mechanism.
You can directly use Pulsar's own message call tracking, monitoring, and logging mechanisms.

# Goals

## In Scope

- To implement a pulsar-rpc-client similar to pulsar-client, we can encapsulate request request as a `request message` 
  and send it to `request topic`. This is a pulsar topic dedicated to receiving requests.
- Implement a pulsar-rpc-server. Internally, the pulsar consumer is used to monitor and receive the message in the 
  `request topic`, and custom logic processing is supported. Finally, the processed results (including possible 
  error messages) are encapsulated as `reply message` sent to `reply topic`. This is a pulsar topic dedicated to 
  storing request processed results.

## Out of Scope

None

# High Level Design

### Architecture Diagram

![RPC.drawio](static/img/pcip-1/pulsar_RPC.png)

## Implementing RPC in Pulsar RPC Framework

1. **pulsar-rpc-client side**
  - **Sending request message to request-topic**: The `pulsar-rpc-client` publishes a message to a `request-topic`. 
    This message includes all necessary data for the `pulsar-rpc-server` to process the request.
  - **Waiting for reply message**: The `pulsar-rpc-client` listens to reply topic. And return to the user for 
    processing, the user can even decide how many reply messages a request has to process successfully.

2. **pulsar-rpc-server side**
  - **Receive and process the request message**: The server side receives the message and performs custom processing.
  - **Send reply message to reply-topic**: Send reply message(it could also be an error message) to reply-topic.

### Handling Timeouts

- The timeout period is set through the pulsar-rpc-client api, which can set a different timeout period for each 
  message. If the timeout period is exceeded, a TimeoutException is returned. And can be returned to the user for 
  the corresponding custom processing.
- There are two cases of timeout. One is that the request task has timed out after the message is normally send 
  to the `request topic`. This situation will allow the consumer to continue processing, but the `pulsar-rpc-client` 
  will be directly returned to the application TimeoutException. When processing each message, the `pulsar-rpc-server` 
  side will obtain the time of initiating the request (or use receivedMsg.getPublishTime()) plus the timeout in the
  message properties. If it is less than or equal to the current time, then do not process and just ACK this message.
- The other is to time out when doing application customization processing on the consumer side. If, after processing, 
  it is checked whether the current is greater than the time to initiate the request plus the timeout in the message 
  properties. If it is greater than then just ACK message, although it has timed out, the message is "discarded", and 
  more importantly, the `pulsar-rpc-server` needs to execution rollback logic(This is defined by the user when they 
  first create the `pulsar-rpc-server`). Because if the status of some data on the business is changed,
  but the request times out, it is considered a call failure and needs to be rollback.

# Detailed Design

## Design & Implementation Details

**Let's take a simple example to see how we can use the new SDK:**
```java
@Test
public void testRpcCall() throws Exception {
    setupTopic("testRpcCall");
    Map<String, Object> requestProducerConfigMap = new HashMap<>();
    requestProducerConfigMap.put("producerName", "requestProducer");
    requestProducerConfigMap.put("messageRoutingMode", MessageRoutingMode.RoundRobinPartition);

    // 1.Create PulsarRpcClient
    rpcClient = createPulsarRpcClient(pulsarClient, requestProducerConfigMap, null, null);

    // 2.Create PulsarRpcServerImpl
    final int defaultEpoch = 1;
    AtomicInteger epoch = new AtomicInteger(defaultEpoch);
    // What do we do when we receive the request message
    requestFunction = request -> {
        epoch.getAndIncrement();
        return CompletableFuture.completedFuture(new TestReply(request.value() + "-----------done"));
    };
    // If the server side is stateful, an error occurs after the server side executes 3-1, and a mechanism for
    // checking and rolling back needs to be provided.
    rollbackFunction = (id, request) -> {
        if (epoch.get() != defaultEpoch) {
            epoch.set(defaultEpoch);
        }
    };
    rpcServer = createPulsarRpcServer(pulsarClient, requestSubBase, requestFunction, rollbackFunction, null);
    ConcurrentHashMap<String, TestReply> resultMap = new ConcurrentHashMap<>();

    Map<String, Object> requestMessageConfigMap = new HashMap<>();
    requestMessageConfigMap.put(TypedMessageBuilder.CONF_DISABLE_REPLICATION, true);

    // 3-1.Synchronous Send
    for (int i = 0; i < messageNum; i++) {
        String correlationId = correlationIdSupplier.get();
        TestRequest message = new TestRequest(synchronousMessage + i);
        requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());

        TestReply reply = rpcClient.request(correlationId, message, requestMessageConfigMap);
        resultMap.put(correlationId, reply);
        log.info("[Synchronous] Reply message: {}, KEY: {}", reply.value(), correlationId);
        rpcClient.removeRequest(correlationId);
    }

    // 3-2.Asynchronous Send
    for (int i = 0; i < messageNum; i++) {
        String asyncCorrelationId = correlationIdSupplier.get();
        TestRequest message = new TestRequest(asynchronousMessage + i);
        requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());

        rpcClient.requestAsync(asyncCorrelationId, message).whenComplete((replyMessage, e) -> {
            if (e != null) {
                log.error("error", e);
            } else {
                resultMap.put(asyncCorrelationId, replyMessage);
                log.info("[Asynchronous] Reply message: {}, KEY: {}", replyMessage.value(), asyncCorrelationId);
            }
            rpcClient.removeRequest(asyncCorrelationId);
        });
    }
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> resultMap.size() == messageNum * 2);
}
```

## Public-facing Changes

### Public API

**@param \<T> The type of the request messages.**

**@param \<V> The type of the reply messages.**

**1.PulsarRpcClientBuilder:** Builder class for constructing a {@link PulsarRpcClient} instance. This builder allows for
the customization of various components required to establish a Pulsar RPC client, including schemas for serialization,
topic details, and timeout configurations.

```java
public interface PulsarRpcClientBuilder<T, V> {

    /**
     * Specifies the Pulsar topic that this client will send to for requests.
     *
     * @param requestTopic the Pulsar topic name
     * @return this builder instance for chaining
     */
    PulsarRpcClientBuilder<T, V> requestTopic(String requestTopic);

    /**
     * Specifies the producer configuration map for request messages.
     *
     * @param requestProducerConfig Configuration map for creating a request message
     *                  producer, will call {@link org.apache.pulsar.client.api.ProducerBuilder#loadConf(java.util.Map)}
     * @return this builder instance for chaining
     */
    PulsarRpcClientBuilder<T, V> requestProducerConfig(@NonNull Map<String, Object> requestProducerConfig);

    /**
     * Sets the topic on which reply messages will be sent.
     *
     * @param replyTopic the topic for reply messages
     * @return this builder instance for chaining
     */
    PulsarRpcClientBuilder<T, V> replyTopic(@NonNull String replyTopic);

    /**
     * Sets the pattern to subscribe to multiple reply topics dynamically.
     *
     * @param replyTopicsPattern the pattern matching reply topics
     * @return this builder instance for chaining
     */
    PulsarRpcClientBuilder<T, V> replyTopicsPattern(@NonNull Pattern replyTopicsPattern);

    /**
     * Specifies the subscription name to use for reply messages.
     *
     * @param replySubscription the subscription name for reply messages
     * @return this builder instance for chaining
     */
    PulsarRpcClientBuilder<T, V> replySubscription(@NonNull String replySubscription);

    /**
     * Sets the timeout for reply messages.
     *
     * @param replyTimeout the duration to wait for a reply before timing out
     * @return this builder instance for chaining
     */
    PulsarRpcClientBuilder<T, V> replyTimeout(@NonNull Duration replyTimeout);

    /**
     * Sets the interval for auto-discovery of topics matching the pattern.
     *
     * @param patternAutoDiscoveryInterval the interval for auto-discovering topics
     * @return this builder instance for chaining
     */
    PulsarRpcClientBuilder<T, V> patternAutoDiscoveryInterval(@NonNull Duration patternAutoDiscoveryInterval);

    /**
     * Sets the {@link RequestCallBack<V>} handler for various request and reply events.
     *
     * @param callBack the callback handler to manage events
     * @return this builder instance for chaining
     */
    PulsarRpcClientBuilder<T, V> requestCallBack(@NonNull RequestCallBack<V> callBack);

    /**
     * Builds and returns a {@link PulsarRpcClient} configured with the current builder settings.
     *
     * @param pulsarClient the client to use for connecting to server
     * @return a new instance of {@link PulsarRpcClient}
     * @throws PulsarRpcClientException if an error occurs during the building of the {@link PulsarRpcClient}
     */
    PulsarRpcClient<T, V> build(PulsarClient pulsarClient) throws PulsarRpcClientException;
}
```

**2.PulsarRpcClient:** Provides the functionality to send asynchronous requests and handle replies using Apache 
Pulsar as the
messaging system. This client manages request-response interactions ensuring that messages are sent
to the correct topics and handling responses through callbacks.

```java
public interface PulsarRpcClient<T, V> extends AutoCloseable {

    /**
     * Creates a builder for configuring a new {@link PulsarRpcClient}.
     *
     * @return A new instance of {@link PulsarRpcClientBuilder}.
     */
    static <T, V> PulsarRpcClientBuilder<T, V> builder(@NonNull Schema<T> requestSchema,
                                                       @NonNull Schema<V> replySchema) {
        return new PulsarRpcClientBuilderImpl<>(requestSchema, replySchema);
    }

    /**
     * Synchronously sends a request and waits for the replies.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @return The reply value.
     * @throws PulsarRpcClientException if an error occurs during the request or while waiting for the reply.
     */
    default V request(String correlationId, T value) throws PulsarRpcClientException {
        return request(correlationId, value, Collections.emptyMap());
    }

    /**
     * Synchronously sends a request and waits for the replies.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @param config Configuration map for creating a request producer,
     *              will call {@link TypedMessageBuilder#loadConf(Map)}
     * @return The reply value.
     * @throws PulsarRpcClientException if an error occurs during the request or while waiting for the reply.
     */
    V request(String correlationId, T value, Map<String, Object> config) throws PulsarRpcClientException;

    /**
     * Asynchronously sends a request and returns a future that completes with the reply.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @return A CompletableFuture that will complete with the reply value.
     */
    default CompletableFuture<V> requestAsync(String correlationId, T value) {
        return requestAsync(correlationId, value, Collections.emptyMap());
    }

    /**
     * Asynchronously sends a request and returns a future that completes with the reply.
     *
     * @param correlationId A unique identifier for the request.
     * @param value The value used to generate the request message
     * @param config Configuration map for creating a request producer,
     *              will call {@link TypedMessageBuilder#loadConf(Map)}
     * @return A CompletableFuture that will complete with the reply value.
     */
    CompletableFuture<V> requestAsync(String correlationId, T value, Map<String, Object> config);

    /**
     * Removes a request from the tracking map based on its correlation ID.
     *
     * <p>When this method is executed, ReplyListener the received message will not be processed again.
     * You need to make sure that this request has been processed through the callback, or you need to resend it.
     *
     * @param correlationId The correlation ID of the request to remove.
     */
    void removeRequest(String correlationId);

    @VisibleForTesting
    int pendingRequestSize();

    /**
     * Closes this client and releases any resources associated with it. This includes closing any active
     * producers and consumers and clearing pending requests.
     *
     * @throws PulsarRpcClientException if there is an error during the closing process.
     */
    @Override
    void close() throws PulsarRpcClientException;
}
```

**3.PulsarRpcServerBuilder:** Builder class for creating instances of {@link PulsarRpcServer}. This class provides
a fluent API to configure the Pulsar RPC server with necessary schemas, topics, subscriptions,
and other configuration parameters related to Pulsar clients. 

Instances of {@link PulsarRpcServer} are configured to handle RPC requests and replies using Apache Pulsar
as the messaging system. This builder allows you to specify the request and reply topics,
schemas for serialization and deserialization, and other relevant settings.

```java
public interface PulsarRpcServerBuilder<T, V> {

    /**
     * Specifies the Pulsar topic that this server will listen to for receiving requests.
     *
     * @param requestTopic the Pulsar topic name
     * @return this builder instance
     */
    PulsarRpcServerBuilder<T, V> requestTopic(@NonNull String requestTopic);

    /**
     * Specifies a pattern for topics that this server will listen to. This is useful for subscribing
     * to multiple topics that match the given pattern.
     *
     * @param requestTopicsPattern the pattern to match topics against
     * @return this builder instance
     */
    PulsarRpcServerBuilder<T, V> requestTopicsPattern(@NonNull Pattern requestTopicsPattern);

    /**
     * Sets the subscription name for this server to use when subscribing to the request topic.
     *
     * @param requestSubscription the subscription name
     * @return this builder instance
     */
    PulsarRpcServerBuilder<T, V> requestSubscription(@NonNull String requestSubscription);

    /**
     * Sets the auto-discovery interval for topics. This setting helps in automatically discovering
     * topics that match the set pattern at the specified interval.
     *
     * @param patternAutoDiscoveryInterval the duration to set for auto-discovery
     * @return this builder instance
     */
    PulsarRpcServerBuilder<T, V> patternAutoDiscoveryInterval(@NonNull Duration patternAutoDiscoveryInterval);

    /**
     * Builds and returns a {@link PulsarRpcServer} instance configured with the current settings of this builder.
     * The server uses provided functional parameters to handle requests and manage rollbacks.
     *
     * @param pulsarClient the client to connect to Pulsar
     * @param requestFunction a function to process incoming requests and generate replies
     * @param rollBackFunction a consumer to handle rollback operations in case of errors
     * @return a new {@link PulsarRpcServer} instance
     * @throws PulsarRpcServerException if an error occurs during server initialization
     */
     PulsarRpcServer<T, V> build(PulsarClient pulsarClient, Function<T, CompletableFuture<V>> requestFunction,
                                     BiConsumer<String, T> rollBackFunction) throws PulsarRpcServerException;

}
```

**4.PulsarRpcServer:** Represents an RPC server that utilizes Apache Pulsar as the messaging layer to handle
request-response cycles in a distributed environment. This server is responsible for
receiving RPC requests, processing them, and sending the corresponding responses back
to the client.

This class integrates tightly with Apache Pulsar's consumer and producer APIs to
receive messages and send replies. It uses a {@link GenericKeyedObjectPool} to manage
a pool of Pulsar producers optimized for sending replies efficiently across different topics.

```java
public interface PulsarRpcServer<T, V> extends AutoCloseable {

    /**
     * Provides a builder to configure and create instances of {@link PulsarRpcServer}.
     *
     * @param requestSchema the schema for serializing and deserializing request messages
     * @param replySchema the schema for serializing and deserializing reply messages
     * @return a builder to configure and instantiate a {@link PulsarRpcServer}
     */
    static <T, V> PulsarRpcServerBuilder<T, V> builder(@NonNull Schema<T> requestSchema,
                                                       @NonNull Schema<V> replySchema) {
        return new PulsarRpcServerBuilderImpl<>(requestSchema, replySchema);
    }

    /**
     * Closes the RPC server, releasing all resources such as the request consumer and reply producer pool.
     * This method ensures that all underlying Pulsar clients are properly closed to free up network resources and
     * prevent memory leaks.
     *
     * @throws PulsarRpcServerException if an error occurs during the closing of server resources
     */
    @Override
    void close() throws PulsarRpcServerException;
}
```

**5.This is the most important interface `RequestCallBack`.**
```java
/**
 * Provides callback methods for various asynchronous events in Pulsar RPC communications.
 * This interface is used to define custom behaviors that occur in response to different stages
 * of message handling, such as request message successful send, send error, successful reply from server,
 * reply error from server, timeouts, and errors in reply message acknowledgment.
 *
 * <p>Implementations of this interface can be used to handle callbacks in a way that integrates
 * seamlessly with business logic, including error handling, logging, or retry mechanisms.</p>
 *
 * @param <V> the type of reply message
 */
public interface RequestCallBack<V> {

    /**
     * Invoked after successfully sending a request to the server.
     *
     * @param correlationId A unique identifier for the request to correlate the response.
     * @param messageId The message ID of the request message sent to server.
     */
    void onSendRequestSuccess(String correlationId, MessageId messageId);

    /**
     * Invoked when an error occurs during the sending of a request message.
     *
     * <p>Please note that {@code replyFuture.completeExceptionally(t)} must be executed at the end.
     *
     * @param correlationId The correlation ID of the request.
     * @param t The throwable error that occurred during sending.
     * @param replyFuture The future where the error will be reported.
     */
    void onSendRequestError(String correlationId, Throwable t, CompletableFuture<V> replyFuture);

    /**
     * Invoked after receiving a reply from the server successfully.
     *
     * <p>Please note that {@code replyFuture.complete(value)} must be executed at the end.
     *
     * @param correlationId The correlation ID associated with the reply.
     * @param subscription The subscription name the reply was received on.
     * @param value The value of the reply.
     * @param replyFuture The future to be completed with the received value.
     */
    void onReplySuccess(String correlationId, String subscription, V value, CompletableFuture<V> replyFuture);

    /**
     * Invoked when an error occurs upon receiving a reply from the server.
     *
     * <p>Please note that {@code replyFuture.completeExceptionally(new Exception(errorMessage))} must be executed
     * at the end.
     *
     * @param correlationId The correlation ID of the request.
     * @param subscription The subscription name the error occurred on.
     * @param errorMessage The error message associated with the reply.
     * @param replyFuture The future to be completed exceptionally due to the error.
     */
    void onReplyError(String correlationId, String subscription, String errorMessage, CompletableFuture<V> replyFuture);

    /**
     * Invoked when receive reply message times out.
     *
     * @param correlationId The correlation ID associated with the request that timed out.
     * @param t The timeout exception or relevant throwable.
     */
    void onTimeout(String correlationId, Throwable t);

    /**
     * Invoked when acknowledging reply message fails.
     *
     * <p>You can retry or record the messageId of the reply message for subsequent processing separately.
     * <p>
     *     This piece does not affect the current function. Because the reply message has been processed by
     *     onReplySuccess or onReplyError. When the user-defined request success condition is met,
     *     the user removes the request through the removeRequest method of rpc client.
     *     Even if you receive the reply message corresponding to this request in the future.
     *     But if there is no request in the pendingRequestMap, it will not be processed.
     * </p>
     *
     * @param correlationId The correlation ID of the message.
     * @param consumer The consumer that is acknowledging the message.
     * @param msg The message that failed to be acknowledged.
     * @param t The throwable error encountered during acknowledgment.
     */
    void onReplyMessageAckFailed(String correlationId, Consumer<V> consumer, Message<V> msg, Throwable t);
}
```

### Binary protocol

### Configuration

### CLI

### Metrics

# Monitoring

No new metrics are added in this proposal.

# Alternatives

None

# General Notes

# Links

* Mailing List discussion thread:
* Mailing List voting thread:
