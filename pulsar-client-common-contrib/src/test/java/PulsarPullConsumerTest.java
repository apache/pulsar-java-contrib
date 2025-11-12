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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarPullConsumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.impl.PulsarPullConsumerImpl;
import org.apache.pulsar.client.common.Constants;
import org.apache.pulsar.client.common.ConsumeStats;
import org.apache.pulsar.client.common.PullRequest;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
public class PulsarPullConsumerTest {
  // todo: We can not add a mock test before the https://github.com/apache/pulsar/pull/24220 is
  // released
  String brokerUrl = "pulsar://127.0.0.1:6650";
  String serviceUrl = "http://127.0.0.1:8080";

  String nonPartitionedTopic = "persistent://public/default/my-topic1";
  String partitionedTopic = "persistent://public/default/my-topic2";
  PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(brokerUrl).build();
  PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(serviceUrl).build();

  public PulsarPullConsumerTest() throws PulsarClientException {}

  @BeforeClass
  public void setup() throws Exception {
    try {
      pulsarAdmin.topics().createNonPartitionedTopic(nonPartitionedTopic);
      pulsarAdmin.topics().createPartitionedTopic(partitionedTopic, 2);
      log.info("Created topics: {}, {}", nonPartitionedTopic, partitionedTopic);
    } catch (Exception e) {
      log.info(
          "Topics already exist, skipping creation: {}, {}", nonPartitionedTopic, partitionedTopic);
    }
  }

  @AfterClass
  public void cleanup() throws Exception {
    pulsarAdmin.topics().delete(nonPartitionedTopic, true);
    pulsarAdmin.topics().deletePartitionedTopic(partitionedTopic, true);
    if (pulsarClient != null) {
      pulsarClient.close();
    }
    if (pulsarAdmin != null) {
      pulsarAdmin.close();
    }
  }

  @DataProvider(name = "testData")
  public Object[][] testData() {
    return new Object[][] {
      {nonPartitionedTopic, Constants.PARTITION_NONE_INDEX},
      {partitionedTopic, 0},
      {partitionedTopic, 1}
    };
  }

  /**
   * Case test design: 1. Single partition topicA 1. Send one thousand messages 2. Create a
   * PullConsumer, subscribe to topicA, and pull messages. 3. Verify message Exactly-once 4. Verify
   * message consumption status 2. Multi-partition topicB 1. Send one thousand messages 3. Create
   * multiple PullConsumers and subscribe to each partition of topicB. 4. Each PullConsumer pulls
   * messages and verifies that the message is Exactly-once. 5. Verify message consumption status
   */
  @Test(dataProvider = "testData")
  public void testPullConsumer(String topic, int partitionIndex) throws Exception {
    log.info("Starting testPullConsumer with topic: {}, partitionIndex: {}", topic, partitionIndex);
    topic =
        partitionIndex == Constants.PARTITION_NONE_INDEX
            ? topic
            : topic + "-partition-" + partitionIndex;
    String subscription = "my-subscription";
    String brokerCluster = "sit";
    @Cleanup
    PulsarPullConsumer<byte[]> pullConsumer =
        new PulsarPullConsumerImpl<>(
            topic,
            subscription,
            brokerCluster,
            Schema.BYTES,
            () -> pulsarClient,
            pulsarAdmin,
            null);
    pullConsumer.start();

    @Cleanup
    Producer<byte[]> producer =
        pulsarClient.newProducer(Schema.BYTES).topic(topic).enableBatching(false).create();

    Set<String> sent = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      String message = "Hello-Pulsar-" + i;
      MessageId messageId = producer.send(message.getBytes());
      sent.add(message);
      log.info("Sent message: {} with id: {}", message, messageId);
    }

    ConsumeStats consumeStats = pullConsumer.getConsumeStats(partitionIndex);
    long offset = consumeStats.getLastConsumedOffset();
    Set<String> received = new HashSet<>();
    while (true) {
      List<Message<byte[]>> messages =
          pullConsumer
              .pull(
                  PullRequest.builder()
                      .offset(offset)
                      .partition(partitionIndex)
                      .maxMessages(10)
                      .maxBytes(1024 * 1024)
                      .timeout(java.time.Duration.ofSeconds(10))
                      .build())
              .getMessages();
      log.info("Pulled {} messages from topic {}", messages.size(), topic);
      if (messages.isEmpty()) {
        log.info("No more messages to pull, exiting...");
        break;
      }
      for (Message<byte[]> message : messages) {
        if (!received.add(new String(message.getData()))) {
          log.error("Duplicate message detected: {}", new String(message.getData()));
        }
      }
      long consumedIndex = messages.get(messages.size() - 1).getIndex().get();
      pullConsumer.ack(consumedIndex, partitionIndex);
      offset = consumedIndex + 1;
      log.info("Acknowledged messages up to index: {}", consumedIndex);
    }
    offset = pullConsumer.getConsumeStats(partitionIndex).getLastConsumedOffset();
    log.info("Final consume offset for non-partitioned topic: {}", offset);
    log.info(
        "received {} unique messages from non-partitioned topic, it is equals to sent {}",
        received.size(),
        received.equals(sent));
    assert received.equals(sent) : "Received messages do not match sent messages";
  }

  @Test(dataProvider = "testData")
  public void testSearchOffset(String topic, int partitionIndex) throws Exception {
    topic =
        partitionIndex == Constants.PARTITION_NONE_INDEX
            ? topic
            : topic + "-partition-" + partitionIndex;
    String subscription = "my-subscription";
    String brokerCluster = "sit";
    @Cleanup
    PulsarPullConsumer<byte[]> pullConsumer =
        new PulsarPullConsumerImpl<>(
            topic,
            subscription,
            brokerCluster,
            Schema.BYTES,
            () -> pulsarClient,
            pulsarAdmin,
            null);
    pullConsumer.start();

    @Cleanup
    Producer<byte[]> producer =
        pulsarClient.newProducer(Schema.BYTES).topic(topic).enableBatching(false).create();

    long timestamp = 0;
    MessageIdAdv messageId = null;
    for (int i = 0; i < 10; i++) {
      String message = "Hello-Pulsar-" + i;
      timestamp = System.currentTimeMillis();
      messageId = (MessageIdAdv) producer.send(message.getBytes());
    }
    for (int i = 0; i < 10; i++) {
      String message = "Hello-Pulsar-" + i;
      producer.send(message.getBytes());
      System.currentTimeMillis();
    }
    long offset = pullConsumer.searchOffset(partitionIndex, timestamp);
    MessageIdAdv searchedMessageId =
        (MessageIdAdv) pulsarAdmin.topics().getMessageIdByIndex(topic, offset);
    assert messageId.getEntryId() == searchedMessageId.getEntryId()
            && messageId.getLedgerId() == searchedMessageId.getLedgerId()
        : "Searched message ID does not match expected message ID";
  }

  @Test
  public void testGetConsumeStats() {
    try {
      String subscription = "test-subscription";
      String brokerCluster = "sit";
      @Cleanup
      PulsarPullConsumer<byte[]> pullConsumer =
          new PulsarPullConsumerImpl<>(
              nonPartitionedTopic,
              subscription,
              brokerCluster,
              Schema.BYTES,
              () -> pulsarClient,
              pulsarAdmin,
              null);
      pullConsumer.start();

      @Cleanup
      Producer<byte[]> producer =
          pulsarClient
              .newProducer(Schema.BYTES)
              .topic(nonPartitionedTopic)
              .enableBatching(false)
              .create();

      for (int i = 0; i < 10; i++) {
        String message = "Hello-Pulsar-" + i;
        producer.send(message.getBytes());
      }

      ConsumeStats offset = pullConsumer.getConsumeStats(Constants.PARTITION_NONE_INDEX);
      log.info("Initial consume offset for topic {}: {}", nonPartitionedTopic, offset);
      Assert.assertEquals(offset.getLastConsumedOffset(), -1L);
      Assert.assertEquals(offset.getMaxOffset(), 9L);
      Assert.assertEquals(offset.getMinOffset(), -1L);
      // Simulate some message processing
      pullConsumer.ack(offset.getLastConsumedOffset() + 10, Constants.PARTITION_NONE_INDEX);
      Thread.sleep(1000);
      ConsumeStats newOffset = pullConsumer.getConsumeStats(Constants.PARTITION_NONE_INDEX);
      Assert.assertEquals(newOffset.getLastConsumedOffset(), 9L);
      Assert.assertEquals(newOffset.getMaxOffset(), 9L);
      Assert.assertEquals(newOffset.getMinOffset(), -1L);
      log.info("New consume offset after ack: {}", newOffset);
    } catch (Exception e) {
      log.error("Error during testExamineConsumeStats", e);
    }
  }
}
