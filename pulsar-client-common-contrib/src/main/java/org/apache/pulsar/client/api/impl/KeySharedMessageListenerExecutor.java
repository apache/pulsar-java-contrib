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
package org.apache.pulsar.client.api.impl;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;
import org.apache.pulsar.client.util.ExecutorProvider;

public class KeySharedMessageListenerExecutor implements MessageListenerExecutor {
  ExecutorProvider executorProvider;

  public KeySharedMessageListenerExecutor(int numThreads, String subscriptionName) {
    this.executorProvider =
        new ExecutorProvider(numThreads, subscriptionName + "-listener-executor");
  }

  @Override
  public void execute(Message<?> message, Runnable runnable) {
    byte[] key = "".getBytes(StandardCharsets.UTF_8);
    if (message.hasKey()) {
      key = message.getKeyBytes();
    } else if (message.hasOrderingKey()) {
      key = message.getOrderingKey();
    }
    // select a thread by message key to execute the runnable!
    // that say, the message listener task with same order key
    // will be executed by the same thread
    ExecutorService executorService = executorProvider.getExecutor(key);
    // executorService is a SingleThreadExecutor
    executorService.execute(runnable);
  }
}
