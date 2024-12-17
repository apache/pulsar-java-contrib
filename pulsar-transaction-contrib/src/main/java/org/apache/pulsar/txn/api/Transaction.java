package org.apache.pulsar.txn.api;

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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * Interface representing an optimized and extended transaction interface in the Pulsar
 * contributors' library.
 *
 * <p>This interface provides enhancements and extensions to the base transaction interface in
 * Pulsar. It specifically addresses the issue where using CumulativeAck with transactions could not
 * prevent message consumed repeated. Additionally, it clarifies and optimizes ambiguous methods for
 * better usability and clarity.
 */
public interface Transaction {

  /**
   * Records a message in the transaction.
   *
   * <p>This method is used to include a message in the current transaction. The message will not be
   * automatically acknowledged when the transaction is committed. Instead, it must be explicitly
   * acknowledged by calling one of the ack methods.
   *
   * @param messageId the ID of the message to record
   * @param consumer the consumer that received the message
   */
  void recordMsg(MessageId messageId, Consumer<?> consumer);

  /**
   * Asynchronously acknowledges all received messages for a specific consumer in the transaction.
   *
   * <p>This method is used to acknowledge all messages that have been recorded for the specified
   * consumer in the transaction. The acknowledgment is asynchronous, and the future can be used to
   * determine when the operation is complete.
   *
   * @param consumer the consumer that received the messages
   * @return a CompletableFuture that will be completed when the acknowledgment is complete
   */
  CompletableFuture<Void> ackAllReceivedMsgsAsync(Consumer<?> consumer);

  /**
   * Acknowledges all received messages for a specific consumer in the transaction.
   *
   * <p>This method is a synchronous version of {@link #ackAllReceivedMsgsAsync(Consumer)}. It will
   * block until the acknowledgment is complete.
   *
   * @param consumer the consumer that received the messages
   * @throws ExecutionException if the acknowledgment fails
   * @throws InterruptedException if the thread is interrupted while waiting for the acknowledgment
   *     to complete
   */
  void ackAllReceivedMsgs(Consumer<?> consumer) throws ExecutionException, InterruptedException;

  /**
   * Acknowledges all received messages in the transaction.
   *
   * <p>This method is a convenience method that acknowledges all messages across all consumers. It
   * will block until the acknowledgment is complete.
   *
   * @throws ExecutionException if the acknowledgment fails
   * @throws InterruptedException if the thread is interrupted while waiting for the acknowledgment
   *     to complete
   */
  void ackAllReceivedMsgs() throws ExecutionException, InterruptedException;

  /**
   * Asynchronously acknowledges all received messages in the transaction.
   *
   * <p>This method is a convenience method that acknowledges all messages across all consumers. The
   * acknowledgment is asynchronous, and the future can be used to determine when the operation is
   * complete.
   *
   * @return a CompletableFuture that will be completed when the acknowledgment is complete
   */
  CompletableFuture<Void> ackAllReceivedMsgsAsync();

  /**
   * Creates a new transactional message builder for the given producer.
   *
   * <p>This method returns a {@link TypedMessageBuilder} instance that is bound to the specified
   * producer and transaction. The returned message builder can be used to construct and send
   * messages within the context of a transaction.
   *
   * @param producer the producer instance used to send messages
   * @param <T> the type of messages produced by the producer
   * @return a TypedMessageBuilder instance for building transactional messages
   */
  <T> TypedMessageBuilder<T> newTransactionMessage(Producer<T> producer);

  /**
   * Asynchronously commits the transaction.
   *
   * <p>This method is used to commit the transaction, making all sent messages and acknowledgments
   * effective. When the transaction is committed, consumers receive the transaction messages and
   * the pending-ack state becomes ack state. The commit is asynchronous, and the future can be used
   * to determine when the operation is complete.
   *
   * @return a CompletableFuture that will be completed when the commit is complete
   */
  CompletableFuture<Void> commitAsync();

  /**
   * Asynchronously aborts the transaction.
   *
   * <p>This method is used to abort the transaction, discarding all send messages and
   * acknowledgments. The abort is asynchronous, and the future can be used to determine when the
   * operation is complete.
   *
   * @return a CompletableFuture that will be completed when the abort is complete
   */
  CompletableFuture<Void> abortAsync();

  /**
   * Commits the transaction.
   *
   * <p>This method is a synchronous version of {@link #commitAsync()}. It will block until the
   * commit is complete.
   *
   * @throws ExecutionException if the commit fails
   * @throws InterruptedException if the thread is interrupted while waiting for the commit to
   *     complete
   */
  void commit() throws ExecutionException, InterruptedException;

  /**
   * Aborts the transaction.
   *
   * <p>This method is a synchronous version of {@link #abortAsync()}. It will block until the abort
   * is complete.
   *
   * @throws ExecutionException if the abort fails
   * @throws InterruptedException if the thread is interrupted while waiting for the abort to
   *     complete
   */
  void abort() throws ExecutionException, InterruptedException;

  /**
   * Gets the transaction ID.
   *
   * <p>This method returns the unique identifier for the transaction.
   *
   * @return the transaction ID
   */
  TxnID getTxnID();

  /**
   * Gets the current state of the transaction.
   *
   * <p>This method returns the current state of the transaction, which can be used to determine if
   * the transaction is open, committed, aborted, error or timeout.
   *
   * @return the current state of the transaction
   */
  org.apache.pulsar.client.api.transaction.Transaction.State getState();
}
