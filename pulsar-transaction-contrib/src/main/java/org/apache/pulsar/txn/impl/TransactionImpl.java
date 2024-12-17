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

package org.apache.pulsar.txn.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.txn.api.Transaction;

public class TransactionImpl implements Transaction {
  @Getter
  ConcurrentHashMap<Consumer<?>, List<MessageId>> receivedMessages = new ConcurrentHashMap<>();

  org.apache.pulsar.client.api.transaction.Transaction transaction;

  public TransactionImpl(org.apache.pulsar.client.api.transaction.Transaction transaction) {
    this.transaction = transaction;
  }

  @Override
  public void recordMsg(MessageId messageId, Consumer<?> consumer) {
    receivedMessages.computeIfAbsent(consumer, k -> new CopyOnWriteArrayList<>()).add(messageId);
  }

  @Override
  public CompletableFuture<Void> ackAllReceivedMsgsAsync(Consumer<?> consumer) {
    List<MessageId> messageIds = receivedMessages.remove(consumer);
    if (messageIds != null) {
      return consumer.acknowledgeAsync(messageIds, transaction);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void ackAllReceivedMsgs(Consumer<?> consumer)
      throws ExecutionException, InterruptedException {
    ackAllReceivedMsgsAsync(consumer).get();
  }

  @Override
  public void ackAllReceivedMsgs() throws ExecutionException, InterruptedException {
    ackAllReceivedMsgsAsync().get();
  }

  @Override
  public CompletableFuture<Void> ackAllReceivedMsgsAsync() {
    return FutureUtil.waitForAll(
        receivedMessages.keySet().stream()
            .map(this::ackAllReceivedMsgsAsync)
            .collect(Collectors.toList()));
  }

  @Override
  public <T> TypedMessageBuilder<T> newTransactionMessage(Producer<T> producer) {
    return producer.newMessage(transaction);
  }

  @Override
  public CompletableFuture<Void> commitAsync() {
    return transaction.commit();
  }

  @Override
  public CompletableFuture<Void> abortAsync() {
    return transaction.abort();
  }

  @Override
  public void commit() throws ExecutionException, InterruptedException {
    transaction.commit().get();
  }

  @Override
  public void abort() throws ExecutionException, InterruptedException {
    transaction.abort().get();
  }

  @Override
  public TxnID getTxnID() {
    return transaction.getTxnID();
  }

  @Override
  public org.apache.pulsar.client.api.transaction.Transaction.State getState() {
    return transaction.getState();
  }
}
