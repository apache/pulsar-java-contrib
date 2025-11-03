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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.rpc.contrib.base.PulsarRpcBase;
import org.apache.pulsar.rpc.contrib.client.PulsarRpcClient;
import org.apache.pulsar.rpc.contrib.client.RequestCallBack;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class PulsarRpcClientTest extends PulsarRpcBase {

  @BeforeMethod(alwaysRun = true)
  protected void setup() throws Exception {
    super.internalSetup();
  }

  @AfterMethod(alwaysRun = true)
  protected void cleanup() throws Exception {
    super.internalCleanup();
  }

  @Test
  public void testPulsarRpcClient() throws Exception {
    setupTopic("testPulsarRpcClient");
    Map<String, Object> requestProducerConfigMap = new HashMap<>();
    requestProducerConfigMap.put("producerName", "requestProducer");
    requestProducerConfigMap.put("messageRoutingMode", MessageRoutingMode.RoundRobinPartition);
    rpcClient =
        PulsarRpcClient.builder(requestSchema, replySchema)
            .requestTopic(requestTopic)
            .requestProducerConfig(requestProducerConfigMap)
            .replyTopic(replyTopic)
            .replySubscription(replySubBase)
            .replyTimeout(replyTimeout)
            .patternAutoDiscoveryInterval(Duration.ofSeconds(1))
            .build(pulsarClient);

    sendRequest();
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> rpcClient.pendingRequestSize() == messageNum);
  }

  @Test
  public void testPulsarRpcClientWithReplyTopicsPattern() throws Exception {
    setupTopic("pattern");
    Map<String, Object> requestProducerConfigMap = new HashMap<>();
    requestProducerConfigMap.put("producerName", "requestProducer");
    requestProducerConfigMap.put("messageRoutingMode", MessageRoutingMode.RoundRobinPartition);
    rpcClient =
        PulsarRpcClient.builder(requestSchema, replySchema)
            .requestTopic(requestTopic)
            .requestProducerConfig(requestProducerConfigMap)
            .replyTopicsPattern(replyTopicPattern)
            .replySubscription(replySubBase)
            .replyTimeout(replyTimeout)
            .patternAutoDiscoveryInterval(Duration.ofSeconds(1))
            .build(pulsarClient);

    sendRequest();
    Awaitility.await()
        .atMost(30, TimeUnit.SECONDS)
        .until(() -> rpcClient.pendingRequestSize() == messageNum);
  }

  @Test
  public void testPulsarRpcClientWithRequestCallBack() throws Exception {
    setupTopic("testPulsarRpcClientWithRequestCallBack");
    Map<String, Object> requestProducerConfigMap = new HashMap<>();
    requestProducerConfigMap.put("producerName", "requestProducer");
    requestProducerConfigMap.put("messageRoutingMode", MessageRoutingMode.RoundRobinPartition);

    AtomicInteger counter = new AtomicInteger();

    RequestCallBack<TestReply> callBack =
        new RequestCallBack<>() {
          @Override
          public void onSendRequestSuccess(String correlationId, MessageId messageId) {
            log.info(
                "<onSendRequestSuccess> CorrelationId[{}] Send request message success. MessageId: {}",
                correlationId,
                messageId);
          }

          @Override
          public void onSendRequestError(
              String correlationId, Throwable t, CompletableFuture<TestReply> replyFuture) {
            log.warn(
                "<onSendRequestError> CorrelationId[{}] Send request message failed. {}",
                correlationId,
                t.getMessage());
            replyFuture.completeExceptionally(t);
          }

          @Override
          public void onReplySuccess(
              String correlationId,
              String subscription,
              TestReply value,
              CompletableFuture<TestReply> replyFuture) {
            log.info(
                "<onReplySuccess> CorrelationId[{}] Subscription[{}] Receive reply message success. Value: {}",
                correlationId,
                subscription,
                value);
            replyFuture.complete(value);
          }

          @Override
          public void onReplyError(
              String correlationId,
              String subscription,
              String errorMessage,
              CompletableFuture<TestReply> replyFuture) {
            log.warn(
                "<onReplyError> CorrelationId[{}] Subscription[{}] Receive reply message failed. {}",
                correlationId,
                subscription,
                errorMessage);
            replyFuture.completeExceptionally(new Exception(errorMessage));
          }

          @Override
          public void onTimeout(String correlationId, Throwable t) {
            log.warn(
                "<onTimeout> CorrelationId[{}] Receive reply message timed out. {}",
                correlationId,
                t.getMessage());
            counter.incrementAndGet();
          }

          @Override
          public void onReplyMessageAckFailed(
              String correlationId,
              Consumer<TestReply> consumer,
              Message<TestReply> msg,
              Throwable t) {
            consumer
                .acknowledgeAsync(msg.getMessageId())
                .exceptionally(
                    ex -> {
                      log.warn(
                          "<onReplyMessageAckFailed> [{}] [{}] Acknowledging message {} failed again.",
                          msg.getTopicName(),
                          correlationId,
                          msg.getMessageId(),
                          ex);
                      return null;
                    });
          }
        };

    rpcClient =
        PulsarRpcClient.builder(requestSchema, replySchema)
            .requestProducerConfig(requestProducerConfigMap)
            .requestTopic(requestTopic)
            .replyTopic(replyTopic)
            .replySubscription(replySubBase)
            .replyTimeout(replyTimeout)
            .patternAutoDiscoveryInterval(Duration.ofSeconds(1))
            .requestCallBack(callBack)
            .build(pulsarClient);

    sendRequest();
    Awaitility.await().atMost(30, TimeUnit.SECONDS).until(() -> counter.get() == messageNum + 1);
  }

  private void sendRequest() {
    Map<String, Object> requestMessageConfigMap = new HashMap<>();
    requestMessageConfigMap.put(TypedMessageBuilder.CONF_DISABLE_REPLICATION, true);

    // Synchronous Send
    String correlationId = correlationIdSupplier.get();
    TestRequest requestMessage = new TestRequest(synchronousMessage);
    requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());
    try {
      rpcClient.request(correlationId, requestMessage, requestMessageConfigMap);
      log.info("[Synchronous] Reply message: {}, KEY: {}", requestMessage, correlationId);
      Assert.fail("Because we only started the PulsarRpcClient. Request will timed out.");
    } catch (Exception e) {
      log.error("An unexpected error occurred while sending the request: " + e.getMessage(), e);
      String expectedMessage = "java.util.concurrent.TimeoutException";
      Assert.assertTrue(
          e.getMessage().contains(expectedMessage),
          "The exception message should contain '" + expectedMessage + "'.");
    }

    // Asynchronous Send
    for (int i = 0; i < messageNum; i++) {
      String asyncCorrelationId = correlationIdSupplier.get();
      TestRequest asyncRequestMessage = new TestRequest(asynchronousMessage + i);
      requestMessageConfigMap.put(TypedMessageBuilder.CONF_EVENT_TIME, System.currentTimeMillis());

      rpcClient
          .requestAsync(asyncCorrelationId, asyncRequestMessage, requestMessageConfigMap)
          .whenComplete(
              (replyMessage, e) -> {
                if (e != null) {
                  log.error("error", e);
                  Assert.fail("Request timed out.");
                } else {
                  log.info(
                      "[Asynchronous] Reply message: {}, KEY: {}",
                      replyMessage.value(),
                      asyncCorrelationId);
                }
                rpcClient.removeRequest(asyncCorrelationId);
              });
    }
  }
}
