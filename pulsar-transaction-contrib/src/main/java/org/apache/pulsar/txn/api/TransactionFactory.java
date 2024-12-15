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
package org.apache.pulsar.txn.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.txn.impl.TransactionImpl;

public class TransactionFactory {

  /**
   * Creates a new transaction with the specified timeout.
   *
   * @param pulsarClient the Pulsar client instance
   * @param timeout the transaction timeout
   * @param unit the time unit of the timeout
   * @return a CompletableFuture that will be completed with the new transaction
   */
  public static CompletableFuture<Transaction> createTransaction(
      PulsarClient pulsarClient, long timeout, TimeUnit unit) {
    // Create a transaction with the specified timeout
    return pulsarClient
        .newTransaction()
        .withTransactionTimeout(timeout, unit)
        .build()
        .thenApply(TransactionImpl::new);
  }
}
