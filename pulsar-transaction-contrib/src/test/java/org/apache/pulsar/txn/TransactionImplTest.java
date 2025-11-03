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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.txn.impl.TransactionImpl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TransactionImplTest {

  private TransactionImpl transactionImpl;
  private org.apache.pulsar.client.api.transaction.Transaction mockTransaction;
  private List<Consumer<?>> mockConsumers;
  private List<MessageId> messageIds;

  @BeforeMethod
  public void setUp() {
    mockTransaction = mock(org.apache.pulsar.client.api.transaction.Transaction.class);
    mockConsumers = new ArrayList<>();
    messageIds = new ArrayList<>();

    // Create two mock consumers and two message IDs
    for (int i = 0; i < 2; i++) {
      Consumer<?> mockConsumer = mock(Consumer.class);
      MessageId messageId = mock(MessageId.class);
      mockConsumers.add(mockConsumer);
      messageIds.add(messageId);
    }

    transactionImpl = new TransactionImpl(mockTransaction);
  }

  @Test
  public void testRecordMsg() {
    // Record a message for a consumer
    Consumer<?> consumer = mockConsumers.get(0);
    MessageId messageId = messageIds.get(0);
    transactionImpl.recordMsg(messageId, consumer);

    // Verify the message is recorded for the consumer
    assertTrue(transactionImpl.getReceivedMessages().get(consumer).contains(messageId));
  }

  @Test
  public void testAckAllReceivedMsgsAsync() throws ExecutionException, InterruptedException {
    // Record messages for different consumers
    for (int i = 0; i < mockConsumers.size(); i++) {
      Consumer<?> consumer = mockConsumers.get(i);
      MessageId messageId = messageIds.get(i);
      transactionImpl.recordMsg(messageId, consumer);
    }

    // Mock the acknowledgeAsync method for each consumer
    for (Consumer<?> consumer : mockConsumers) {
      when(consumer.acknowledgeAsync(anyList(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));
    }

    // Call the ackAllReceivedMsgsAsync method
    CompletableFuture<Void> future = transactionImpl.ackAllReceivedMsgsAsync();
    future.get();

    // Verify each consumer called the correct acknowledgeAsync method with the correct message
    // IDs
    for (int i = 0; i < mockConsumers.size(); i++) {
      Consumer<?> consumer = mockConsumers.get(i);
      MessageId messageId = messageIds.get(i);
      verify(consumer).acknowledgeAsync(eq(List.of(messageId)), eq(mockTransaction));
    }
  }

  @Test
  public void testAckAllReceivedMsgs() throws ExecutionException, InterruptedException {
    // Record messages for a consumer
    Consumer<?> consumer = mockConsumers.get(0);
    MessageId messageId = messageIds.get(0);
    transactionImpl.recordMsg(messageId, consumer);

    // Mock the acknowledgeAsync method for the consumer
    when(consumer.acknowledgeAsync(anyList(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    // Call the ackAllReceivedMsgs method
    transactionImpl.ackAllReceivedMsgs(consumer);

    // Verify the consumer called the correct acknowledgeAsync method with the correct message
    // IDs
    verify(consumer).acknowledgeAsync(eq(List.of(messageId)), eq(mockTransaction));
    // Verify the message Ids were removed from the transaction context after acked.
    assertEquals(transactionImpl.getReceivedMessages().size(), 0);
  }

  @Test
  public void testAckAllReceivedMsgsAll() throws ExecutionException, InterruptedException {
    // Record messages for different consumers
    for (int i = 0; i < mockConsumers.size(); i++) {
      Consumer<?> consumer = mockConsumers.get(i);
      MessageId messageId = messageIds.get(i);
      transactionImpl.recordMsg(messageId, consumer);
    }

    // Mock the acknowledgeAsync method for each consumer
    for (Consumer<?> consumer : mockConsumers) {
      when(consumer.acknowledgeAsync(anyList(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));
    }

    // Call the ackAllReceivedMsgs method
    transactionImpl.ackAllReceivedMsgs();

    // Verify each consumer called the correct acknowledgeAsync method with the correct message
    // IDs
    for (int i = 0; i < mockConsumers.size(); i++) {
      Consumer<?> consumer = mockConsumers.get(i);
      MessageId messageId = messageIds.get(i);
      verify(consumer).acknowledgeAsync(eq(List.of(messageId)), eq(mockTransaction));
    }
  }

  @Test
  public void testAckAllReceivedMsgsAsyncAll() throws ExecutionException, InterruptedException {
    // Record messages for different consumers
    for (int i = 0; i < mockConsumers.size(); i++) {
      Consumer<?> consumer = mockConsumers.get(i);
      MessageId messageId = messageIds.get(i);
      transactionImpl.recordMsg(messageId, consumer);
    }

    // Mock the acknowledgeAsync method for each consumer
    for (Consumer<?> consumer : mockConsumers) {
      when(consumer.acknowledgeAsync(anyList(), any()))
          .thenReturn(CompletableFuture.completedFuture(null));
    }

    // Call the ackAllReceivedMsgsAsync method
    CompletableFuture<Void> future = transactionImpl.ackAllReceivedMsgsAsync();
    future.get();

    // Verify each consumer called the correct acknowledgeAsync method with the correct message
    // IDs
    for (int i = 0; i < mockConsumers.size(); i++) {
      Consumer<?> consumer = mockConsumers.get(i);
      MessageId messageId = messageIds.get(i);
      verify(consumer).acknowledgeAsync(eq(List.of(messageId)), eq(mockTransaction));
    }
  }

  @Test
  public void testCommitAsync() throws ExecutionException, InterruptedException {
    // Mock the commit method of the transaction
    when(mockTransaction.commit()).thenReturn(CompletableFuture.completedFuture(null));

    // Call the commitAsync method
    CompletableFuture<Void> future = transactionImpl.commitAsync();
    future.get();

    // Verify the commit method was called
    verify(mockTransaction).commit();
  }

  @Test
  public void testAbortAsync() throws ExecutionException, InterruptedException {
    // Mock the abort method of the transaction
    when(mockTransaction.abort()).thenReturn(CompletableFuture.completedFuture(null));

    // Call the abortAsync method
    CompletableFuture<Void> future = transactionImpl.abortAsync();
    future.get();

    // Verify the abort method was called
    verify(mockTransaction).abort();
  }

  @Test
  public void testCommit() throws ExecutionException, InterruptedException {
    // Mock the commit method of the transaction
    when(mockTransaction.commit()).thenReturn(CompletableFuture.completedFuture(null));

    // Call the commit method
    transactionImpl.commit();

    // Verify the commit method was called
    verify(mockTransaction).commit();
  }

  @Test
  public void testAbort() throws ExecutionException, InterruptedException {
    // Mock the abort method of the transaction
    when(mockTransaction.abort()).thenReturn(CompletableFuture.completedFuture(null));

    // Call the abort method
    transactionImpl.abort();

    // Verify the abort method was called
    verify(mockTransaction).abort();
  }

  @Test
  public void testGetTxnID() {
    // Mock the getTxnID method of the transaction
    TxnID txnID = mock(TxnID.class);
    when(mockTransaction.getTxnID()).thenReturn(txnID);

    // Call the getTxnID method
    assertEquals(txnID, transactionImpl.getTxnID());
  }

  @Test
  public void testGetState() {
    // Mock the getState method of the transaction
    org.apache.pulsar.client.api.transaction.Transaction.State state =
        org.apache.pulsar.client.api.transaction.Transaction.State.OPEN;
    when(mockTransaction.getState()).thenReturn(state);

    // Call the getState method
    assertEquals(state, transactionImpl.getState());
  }
}
