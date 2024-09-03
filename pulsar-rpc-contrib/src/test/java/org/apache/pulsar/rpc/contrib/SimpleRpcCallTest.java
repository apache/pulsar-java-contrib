/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pulsar.rpc.contrib;

import static java.util.UUID.randomUUID;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.rpc.contrib.client.PulsarRpcClient;
import org.apache.pulsar.rpc.contrib.client.PulsarRpcClientBuilder;
import org.apache.pulsar.rpc.contrib.client.RequestCallBack;
import org.apache.pulsar.rpc.contrib.server.PulsarRpcServer;
import org.apache.pulsar.rpc.contrib.server.PulsarRpcServerBuilder;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class SimpleRpcCallTest {
    private final Supplier<String> correlationIdSupplier = () -> randomUUID().toString();
    private final String topicPrefix = "persistent://public/default/";
    private final String topicBase = "testRpcCall";
    private final String requestTopic = topicBase + "-request";
    private final String replyTopic = topicBase + "-reply";
    Pattern requestTopicPattern = Pattern.compile(topicPrefix + requestTopic + ".*");
    Pattern replyTopicPattern = Pattern.compile(topicPrefix + replyTopic + ".*");
    private final String requestSubBase = requestTopic + "-sub";
    private final String replySubBase = replyTopic + "-sub";
    private final Duration replyTimeout = Duration.ofSeconds(3);
    private final String synchronousMessage = "SynchronousRequest";
    private final String asynchronousMessage = "AsynchronousRequest";
    private final Schema<TestRequest> requestSchema = Schema.JSON(TestRequest.class);
    private final Schema<TestReply> replySchema = Schema.JSON(TestReply.class);

    private PulsarAdmin pulsarAdmin;
    private PulsarClient pulsarClient;
    private PulsarRpcClient<TestRequest, TestReply> rpcClient;
    private PulsarRpcServer<TestRequest, TestReply> rpcServer;

    private Function<TestRequest, CompletableFuture<TestReply>> requestFunction;
    private BiConsumer<String, TestRequest> rollbackFunction;

    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build();
        pulsarAdmin.topics().createPartitionedTopic(requestTopic, 10);
        pulsarAdmin.topics().createPartitionedTopic(replyTopic, 10);
        pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        if (rpcServer != null) {
            rpcServer.close();
        }
        rpcClient.close();
        pulsarClient.close();
        pulsarAdmin.topics().deletePartitionedTopic(requestTopic);
        pulsarAdmin.topics().deletePartitionedTopic(replyTopic);
        pulsarAdmin.close();
    }

    @Test
    public void testRpcCall() throws Exception {
        ProducerBuilder<TestRequest> requestProducerBuilder = pulsarClient.newProducer(requestSchema)
                .topic(requestTopic)
                .enableBatching(false)
                .producerName("requestProducer");

        // 1.Create PulsarRpcClient
        rpcClient = createPulsarRpcClient(pulsarClient, requestProducerBuilder, null, null);

        // 2.Create PulsarRpcServer
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
        int messageNum = 10;
        // 3-1.Synchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            TypedMessageBuilder<TestRequest> message = newRequestMessage(rpcClient.getRequestProducer(),
                    correlationId, synchronousMessage + i);
            TestReply reply = rpcClient.request(correlationId, message);
            resultMap.put(correlationId, reply);
            log.info("[Synchronous] Reply message: {}, KEY: {}", reply.value(), correlationId);
        }

        // 3-2.Asynchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            TypedMessageBuilder<TestRequest> message = newRequestMessage(rpcClient.getRequestProducer(),
                    correlationId, asynchronousMessage + i);
            rpcClient.requestAsync(correlationId, message).whenComplete((replyMessage, e) -> {
                if (e != null) {
                    log.error("error", e);
                } else {
                    resultMap.put(correlationId, replyMessage);
                    log.info("[Asynchronous] Reply message: {}, KEY: {}", replyMessage.value(), correlationId);
                }
            });
        }
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> resultMap.size() == messageNum * 2);
    }

    @Test
    public void testRpcCallWithCallBack() throws Exception {
        ProducerBuilder<TestRequest> requestProducerBuilder = pulsarClient.newProducer(requestSchema)
                .topic(requestTopic)
                .enableBatching(false)
                .producerName("requestProducer");

        AtomicInteger counter = new AtomicInteger();

        RequestCallBack<TestReply> callBack = new RequestCallBack<>() {
            @Override
            public void onSendRequestSuccess(String correlationId, MessageId messageId) {
                log.info("<onSendRequestSuccess> CorrelationId[{}] Send request message success. MessageId: {}",
                        correlationId, messageId);
            }

            @Override
            public void onSendRequestError(String correlationId, Throwable t,
                                           CompletableFuture<TestReply> replyFuture) {
                log.warn("<onSendRequestError> CorrelationId[{}] Send request message failed. {}",
                        correlationId, t.getMessage());
                replyFuture.completeExceptionally(t);
            }

            @Override
            public void onReplySuccess(String correlationId, String subscription,
                                       TestReply value, CompletableFuture<TestReply> replyFuture) {
                log.info("<onReplySuccess> CorrelationId[{}] Subscription[{}] Receive reply message success. Value: {}",
                        correlationId, subscription, value);
                counter.incrementAndGet();
                replyFuture.complete(value);
            }

            @Override
            public void onReplyError(String correlationId, String subscription,
                                     String errorMessage, CompletableFuture<TestReply> replyFuture) {
                log.warn("<onReplyError> CorrelationId[{}] Subscription[{}] Receive reply message failed. {}",
                        correlationId, subscription, errorMessage);
                replyFuture.completeExceptionally(new Exception(errorMessage));
            }

            @Override
            public void onTimeout(String correlationId, Throwable t) {
                log.warn("<onTimeout> CorrelationId[{}] Receive reply message timed out. {}",
                        correlationId, t.getMessage());
            }

            @Override
            public void onReplyMessageAckFailed(String correlationId, Consumer<TestReply> consumer,
                                                Message<TestReply> msg, Throwable t) {
                consumer.acknowledgeAsync(msg.getMessageId()).exceptionally(ex -> {
                    log.warn("<onReplyMessageAckFailed> [{}] [{}] Acknowledging message {} failed again.",
                            msg.getTopicName(), correlationId, msg.getMessageId(), ex);
                    return null;
                });
            }
        };

        rpcClient = createPulsarRpcClient(pulsarClient, requestProducerBuilder, null, callBack);

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
        PulsarRpcServer<TestRequest, TestReply> rpcServer1 = createPulsarRpcServer(pulsarClient, requestSubBase + "-1",
                requestFunction, rollbackFunction, null);
        PulsarRpcServer<TestRequest, TestReply> rpcServer2 = createPulsarRpcServer(pulsarClient, requestSubBase + "-2",
                requestFunction, rollbackFunction, null);
        PulsarRpcServer<TestRequest, TestReply> rpcServer3 = createPulsarRpcServer(pulsarClient, requestSubBase + "-3",
                requestFunction, rollbackFunction, null);

        int messageNum = 10;
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            TypedMessageBuilder<TestRequest> message = newRequestMessage(rpcClient.getRequestProducer(),
                    correlationId, asynchronousMessage + i);
            rpcClient.requestAsync(correlationId, message);
        }
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> counter.get() == messageNum * 3);
        rpcServer1.close();
        rpcServer2.close();
        rpcServer3.close();
    }

    @Test
    public void testRpcCallWithPattern() throws Exception {
        ProducerBuilder<TestRequest> requestProducerBuilder = pulsarClient.newProducer(requestSchema)
                .topic(requestTopic)
                .enableBatching(false)
                .producerName("requestProducer");

        // 1.Create PulsarRpcClient
        rpcClient = createPulsarRpcClient(pulsarClient, requestProducerBuilder, replyTopicPattern, null);

        // 2.Create PulsarRpcServer
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
        rpcServer = createPulsarRpcServer(pulsarClient, requestSubBase, requestFunction, rollbackFunction,
                requestTopicPattern);

        ConcurrentHashMap<String, TestReply> resultMap = new ConcurrentHashMap<>();
        int messageNum = 10;
        // 3-1.Synchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            TypedMessageBuilder<TestRequest> message = newRequestMessage(rpcClient.getRequestProducer(),
                    correlationId, synchronousMessage + i);
            TestReply reply = rpcClient.request(correlationId, message);
            resultMap.put(correlationId, reply);
            log.info("[Synchronous] Reply message: {}, KEY: {}", reply.value(), correlationId);
        }

        // 3-2.Asynchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            TypedMessageBuilder<TestRequest> message = newRequestMessage(rpcClient.getRequestProducer(),
                    correlationId, asynchronousMessage + i);
            rpcClient.requestAsync(correlationId, message).whenComplete((replyMessage, e) -> {
                if (e != null) {
                    log.error("error", e);
                } else {
                    resultMap.put(correlationId, replyMessage);
                    log.info("[Asynchronous] Reply message: {}, KEY: {}", replyMessage.value(), correlationId);
                }
            });
        }
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> resultMap.size() == messageNum * 2);
    }

    @Test
    public void testRpcCallTimeout() throws Exception {
        ProducerBuilder<TestRequest> requestProducerBuilder = pulsarClient.newProducer(requestSchema)
                .topic(requestTopic)
                .enableBatching(false)
                .producerName("requestProducer");

        // 1.Create PulsarRpcClient
        rpcClient = createPulsarRpcClient(pulsarClient, requestProducerBuilder, null, null);

        // 2.Create PulsarRpcServer
        final int defaultEpoch = 1;
        AtomicInteger epoch = new AtomicInteger(defaultEpoch);
        // What do we do when we receive the request message
        requestFunction = request -> {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
        int messageNum = 1;
        // 3-1.Synchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            try {
                TypedMessageBuilder<TestRequest> message = newRequestMessage(rpcClient.getRequestProducer(),
                        correlationId, synchronousMessage + i);
                TestReply reply = rpcClient.request(correlationId, message);
                resultMap.put(correlationId, reply);
                log.info("[Synchronous] Reply message: {}, KEY: {}", reply.value(), correlationId);
                Assert.fail("Request timed out.");
            } catch (Exception e) {
                log.error("An unexpected error occurred while sending the request: " + e.getMessage(), e);
                resultMap.put(correlationId, new TestReply(e.getMessage()));
            }
        }

        // 3-2.Asynchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            TypedMessageBuilder<TestRequest> message = newRequestMessage(rpcClient.getRequestProducer(),
                    correlationId, asynchronousMessage + i);
            rpcClient.requestAsync(correlationId, message).whenComplete((replyMessage, e) -> {
                if (e != null) {
                    log.error("error", e);
                    resultMap.put(correlationId, new TestReply(e.getMessage()));
                    Assert.fail("Request timed out.");
                } else {
                    resultMap.put(correlationId, replyMessage);
                    log.info("[Asynchronous] Reply message: {}, KEY: {}", replyMessage.value(), correlationId);
                }
            });
        }
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> resultMap.size() == messageNum * 2);
    }

    @Test
    public void testRpcCallProcessFailedOnServerSide() throws Exception {
        ProducerBuilder<TestRequest> requestProducerBuilder = pulsarClient.newProducer(requestSchema)
                .topic(requestTopic)
                .enableBatching(false)
                .producerName("requestProducer");

        // 1.Create PulsarRpcClient
        rpcClient = createPulsarRpcClient(pulsarClient, requestProducerBuilder, null, null);

        // 2.Create PulsarRpcServer
        final int defaultEpoch = 1;
        AtomicInteger epoch = new AtomicInteger(defaultEpoch);
        // What do we do when we receive the request message
        requestFunction = request -> {
            try {
                epoch.getAndIncrement();
                int i = epoch.get() / 0;
                return CompletableFuture.completedFuture(new TestReply(request.value() + "-----------done"));
            } catch (Exception e) {
                return CompletableFuture.failedFuture(e);
            }
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
        int messageNum = 1;
        // 3-1.Synchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            try {
                TypedMessageBuilder<TestRequest> message = newRequestMessage(rpcClient.getRequestProducer(),
                        correlationId, synchronousMessage + i);
                TestReply reply = rpcClient.request(correlationId, message);
                resultMap.put(correlationId, reply);
                log.info("[Synchronous] Reply message: {}, KEY: {}", reply.value(), correlationId);
                Assert.fail("Server process failed.");
            } catch (Exception e) {
                log.error("An unexpected error occurred while sending the request: " + e.getMessage(), e);
                resultMap.put(correlationId, new TestReply(e.getMessage()));
            }
        }

        // 3-2.Asynchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            TypedMessageBuilder<TestRequest> message = newRequestMessage(rpcClient.getRequestProducer(),
                    correlationId, asynchronousMessage + i);
            rpcClient.requestAsync(correlationId, message).whenComplete((replyMessage, e) -> {
                if (e != null) {
                    log.error("error", e);
                    resultMap.put(correlationId, new TestReply(e.getMessage()));
                    Assert.fail("Server process failed.");
                } else {
                    resultMap.put(correlationId, replyMessage);
                    log.info("[Asynchronous] Reply message: {}, KEY: {}", replyMessage.value(), correlationId);
                }
            });
        }
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> resultMap.size() == messageNum * 2);
    }

    private PulsarRpcClient<TestRequest, TestReply> createPulsarRpcClient(
            PulsarClient pulsarClient, ProducerBuilder<TestRequest> requestProducerBuilder,
            Pattern replyTopicsPattern, RequestCallBack<TestReply> callBack) throws IOException {
        PulsarRpcClientBuilder<TestRequest, TestReply> rpcClientBuilder =
                PulsarRpcClient.builder(requestSchema, replySchema)
                        .requestProducer(requestProducerBuilder)
                        .replySubscription(replySubBase)
                        .replyTimeout(replyTimeout)
                        .patternAutoDiscoveryInterval(Duration.ofSeconds(1));
        if (callBack != null) {
            rpcClientBuilder.requestCallBack(callBack);
        }
        return replyTopicsPattern == null ? rpcClientBuilder.replyTopic(replyTopic).build(pulsarClient)
                : rpcClientBuilder.replyTopicsPattern(replyTopicsPattern).build(pulsarClient);

    }

    private TypedMessageBuilder<TestRequest> newRequestMessage(
            Producer<TestRequest> requestProducer,
            String correlationId,
            String value) {
        return requestProducer.newMessage()
                .key(correlationId)
                .value(new TestRequest(value));
    }

    private PulsarRpcServer<TestRequest, TestReply> createPulsarRpcServer(
            PulsarClient pulsarClient, String requestSubscription,
            Function<TestRequest, CompletableFuture<TestReply>> requestFunction,
            BiConsumer<String, TestRequest> rollbackFunction,
            Pattern requestTopicsPattern) throws IOException {
        PulsarRpcServerBuilder<TestRequest, TestReply> rpcServerBuilder =
                PulsarRpcServer.builder(requestSchema, replySchema)
                        .requestSubscription(requestSubscription)
                        .patternAutoDiscoveryInterval(Duration.ofSeconds(1));
        if (requestTopicsPattern == null) {
            rpcServerBuilder.requestTopic(requestTopic);
        } else {
            rpcServerBuilder.requestTopicsPattern(requestTopicsPattern)
            ;
        }
        return rpcServerBuilder.build(pulsarClient, requestFunction, rollbackFunction);
    }

    public record TestRequest(String value) {
    }

    public record TestReply(String value) {
    }

}
