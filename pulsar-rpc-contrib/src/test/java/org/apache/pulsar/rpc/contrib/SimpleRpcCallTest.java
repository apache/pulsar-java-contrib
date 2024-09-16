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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.rpc.contrib.client.PulsarRpcClient;
import org.apache.pulsar.rpc.contrib.client.PulsarRpcClientBuilder;
import org.apache.pulsar.rpc.contrib.client.RequestCallBack;
import org.apache.pulsar.rpc.contrib.common.PulsarRpcClientException;
import org.apache.pulsar.rpc.contrib.common.PulsarRpcServerException;
import org.apache.pulsar.rpc.contrib.server.PulsarRpcServer;
import org.apache.pulsar.rpc.contrib.server.PulsarRpcServerBuilder;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class SimpleRpcCallTest extends PulsarRpcBase {
    private Function<PulsarRpcBase.TestRequest, CompletableFuture<PulsarRpcBase.TestReply>> requestFunction;
    private BiConsumer<String, PulsarRpcBase.TestRequest> rollbackFunction;

    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

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

    @Test
    public void testRpcCallWithCallBack() throws Exception {
        setupTopic("testRpcCallWithCallBack");
        Map<String, Object> requestProducerConfigMap = new HashMap<>();
        requestProducerConfigMap.put("producerName", "requestProducer");
        requestProducerConfigMap.put("messageRoutingMode", MessageRoutingMode.RoundRobinPartition);

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

        rpcClient = createPulsarRpcClient(pulsarClient, requestProducerConfigMap, null, callBack);

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

        Map<String, Object> requestMessageConfigMap = new HashMap<>();
        requestMessageConfigMap.put(TypedMessageBuilder.CONF_DISABLE_REPLICATION, true);
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            TestRequest message = new TestRequest(asynchronousMessage + i);
            requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());
            rpcClient.requestAsync(correlationId, message, requestMessageConfigMap);
        }
        Awaitility.await().atMost(20, TimeUnit.SECONDS).until(() -> counter.get() == messageNum * 3);
        rpcServer1.close();
        rpcServer2.close();
        rpcServer3.close();
    }

    @Test
    public void testRpcCallWithPattern() throws Exception {
        setupTopic("pattern");
        Map<String, Object> requestProducerConfigMap = new HashMap<>();
        requestProducerConfigMap.put("producerName", "requestProducer");
        requestProducerConfigMap.put("messageRoutingMode", MessageRoutingMode.RoundRobinPartition);

        // 1.Create PulsarRpcClient
        rpcClient = createPulsarRpcClient(pulsarClient, requestProducerConfigMap, replyTopicPattern, null);

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
        rpcServer = createPulsarRpcServer(pulsarClient, requestSubBase, requestFunction, rollbackFunction,
                requestTopicPattern);

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
            String correlationId = correlationIdSupplier.get();
            TestRequest message = new TestRequest(asynchronousMessage + i);
            requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());
            rpcClient.requestAsync(correlationId, message, requestMessageConfigMap).whenComplete((replyMessage, e) -> {
                if (e != null) {
                    log.error("error", e);
                } else {
                    resultMap.put(correlationId, replyMessage);
                    log.info("[Asynchronous] Reply message: {}, KEY: {}", replyMessage.value(), correlationId);
                }
                rpcClient.removeRequest(correlationId);
            });
        }
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> resultMap.size() == messageNum * 2);
    }

    @Test
    public void testRpcCallTimeout() throws Exception {
        setupTopic("testRpcCallTimeout");
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
        Map<String, Object> requestMessageConfigMap = new HashMap<>();
        requestMessageConfigMap.put(TypedMessageBuilder.CONF_DISABLE_REPLICATION, true);

        // 3-1.Synchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            try {
                TestRequest message = new TestRequest(synchronousMessage + i);
                requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());
                TestReply reply = rpcClient.request(correlationId, message, requestMessageConfigMap);
                resultMap.put(correlationId, reply);
                log.info("[Synchronous] Reply message: {}, KEY: {}", reply.value(), correlationId);
                Assert.fail("Request timed out.");
            } catch (Exception e) {
                log.error("An unexpected error occurred while sending the request: " + e.getMessage(), e);
                String expectedMessage = "java.util.concurrent.TimeoutException";
                Assert.assertTrue(e.getMessage().contains(expectedMessage),
                        "The exception message should contain '" + expectedMessage + "'.");
                resultMap.put(correlationId, new TestReply(e.getMessage()));
            }
        }

        // 3-2.Asynchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            TestRequest message = new TestRequest(asynchronousMessage + i);
            requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());
            rpcClient.requestAsync(correlationId, message, requestMessageConfigMap).whenComplete((replyMessage, e) -> {
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
        setupTopic("testRpcCallProcessFailedOnServerSide");
        Map<String, Object> requestProducerConfigMap = new HashMap<>();
        requestProducerConfigMap.put("producerName", "requestProducer");
        requestProducerConfigMap.put("messageRoutingMode", MessageRoutingMode.RoundRobinPartition);

        // 1.Create PulsarRpcClient
        rpcClient = createPulsarRpcClient(pulsarClient, requestProducerConfigMap, null, null);

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
        Map<String, Object> requestMessageConfigMap = new HashMap<>();
        requestMessageConfigMap.put(TypedMessageBuilder.CONF_DISABLE_REPLICATION, true);

        // 3-1.Synchronous Send
        for (int i = 0; i < messageNum; i++) {
            String correlationId = correlationIdSupplier.get();
            try {
                TestRequest message = new TestRequest(synchronousMessage + i);
                requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());
                TestReply reply = rpcClient.request(correlationId, message, requestMessageConfigMap);
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
            TestRequest message = new TestRequest(asynchronousMessage + i);
            requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());
            rpcClient.requestAsync(correlationId, message, requestMessageConfigMap).whenComplete((replyMessage, e) -> {
                if (e != null) {
                    log.error("error", e);
                    resultMap.put(correlationId, new TestReply(e.getMessage()));
                    Assert.fail("Server process failed.");
                } else {
                    resultMap.put(correlationId, replyMessage);
                    log.info("[Asynchronous] Reply message: {}, KEY: {}", replyMessage.value(), correlationId);
                    String expectedMessage = "java.lang.ArithmeticException";
                    Assert.assertTrue(e.getMessage().contains(expectedMessage),
                            "The exception message should contain '" + expectedMessage + "'.");
                }
            });
        }
        Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> resultMap.size() == messageNum * 2);
    }

    private PulsarRpcClient<TestRequest, TestReply> createPulsarRpcClient(
            PulsarClient pulsarClient, Map<String, Object> requestProducerConfigMap,
            Pattern replyTopicsPattern, RequestCallBack<TestReply> callBack) throws PulsarRpcClientException {
        PulsarRpcClientBuilder<TestRequest, TestReply> rpcClientBuilder =
                PulsarRpcClient.builder(requestSchema, replySchema)
                        .requestTopic(requestTopic)
                        .requestProducerConfig(requestProducerConfigMap)
                        .replySubscription(replySubBase)
                        .replyTimeout(replyTimeout)
                        .patternAutoDiscoveryInterval(Duration.ofSeconds(1));
        if (callBack != null) {
            rpcClientBuilder.requestCallBack(callBack);
        }
        return replyTopicsPattern == null ? rpcClientBuilder.replyTopic(replyTopic).build(pulsarClient)
                : rpcClientBuilder.replyTopicsPattern(replyTopicsPattern).build(pulsarClient);
    }

    private PulsarRpcServer<TestRequest, TestReply> createPulsarRpcServer(
            PulsarClient pulsarClient, String requestSubscription,
            Function<TestRequest, CompletableFuture<TestReply>> requestFunction,
            BiConsumer<String, TestRequest> rollbackFunction,
            Pattern requestTopicsPattern) throws PulsarRpcServerException {
        PulsarRpcServerBuilder<TestRequest, TestReply> rpcServerBuilder =
                PulsarRpcServer.builder(requestSchema, replySchema)
                        .requestSubscription(requestSubscription)
                        .patternAutoDiscoveryInterval(Duration.ofSeconds(1));
        if (requestTopicsPattern == null) {
            rpcServerBuilder.requestTopic(requestTopic);
        } else {
            rpcServerBuilder.requestTopicsPattern(requestTopicsPattern);
        }
        return rpcServerBuilder.build(pulsarClient, requestFunction, rollbackFunction);
    }

}
