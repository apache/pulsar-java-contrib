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
package org.apache.pulsar.txn;

import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.txn.api.Transaction;
import org.apache.pulsar.txn.api.TransactionFactory;
import org.testng.annotations.Test;

public class TransactionDemo {

  @Test
  public void transactionDemo() throws Exception {
    String pubTopic = "persistent://public/default/my-pub-topic";
    String subTopic = "persistent://public/default/my-sub-topic";
    String subscription = "my-subscription";

    // Create a Pulsar client instance
    PulsarClient client = SingletonPulsarContainer.createPulsarClient();

    // Create a Transaction object
    // Use TransactionFactory to create a transaction object with a timeout of 5 seconds
    Transaction transaction =
            TransactionFactory.createTransaction(client, 5, TimeUnit.SECONDS).get();

    // Create producers and a consumer
    // Create two producers to send messages to different topics
    Producer<String> producerToPubTopic = client.newProducer(Schema.STRING).topic(pubTopic).create();
    Producer<String> producerToSubTopic = client.newProducer(Schema.STRING).topic(subTopic).create();

    // Create a consumer to receive messages from the subTopic
    Consumer<String> consumerFromSubTopic = client
            .newConsumer(Schema.STRING)
            .subscriptionName(subscription)
            .topic(subTopic)
            .subscribe();

    // Send a message to the Sub Topic
    producerToSubTopic.send("Hello World");

    // Receive a message
    Message<String> receivedMessage = consumerFromSubTopic.receive();
    MessageId receivedMessageId = receivedMessage.getMessageId();

    // Record the message in the transaction
    transaction.recordMsg(receivedMessageId, consumerFromSubTopic);

    // Forward the transaction message to the pub topic
    // Use the transaction message builder to forward the received message to the pubTopic
    transaction.newTransactionMessage(producerToSubTopic).value(receivedMessage.getValue()).send();

    // Acknowledge all received messages
    // Acknowledge all messages received from the subTopic within the transaction
    transaction.ackAllReceivedMsgs(consumerFromSubTopic);

    // Commit the transaction
    // Commit the transaction to ensure all recorded messages and acknowledgments take effect
    transaction.commit();

    // Close the consumer, producers, and client to release resources
    consumerFromSubTopic.close();
    producerToSubTopic.close();
    client.close();
  }
}