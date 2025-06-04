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

import java.util.concurrent.ExecutorService;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;
import org.apache.pulsar.client.util.ExecutorProvider;

public class PartitionOrderMessageListenerExecutor implements MessageListenerExecutor {
  private final ExecutorProvider executorProvider;

  public PartitionOrderMessageListenerExecutor(int numThreads, String subscriptionName) {
    this.executorProvider =
        new ExecutorProvider(numThreads, subscriptionName + "-listener-executor");
  }

  @Override
  public void execute(Message<?> message, Runnable runnable) {
    // select a thread by partition topic name to execute the runnable!
    // that say, the message listener task from the same partition topic
    // will be executed by the same thread
    ExecutorService executorService =
        executorProvider.getExecutor(message.getTopicName().getBytes());
    // executorService is a SingleThreadExecutor
    executorService.execute(runnable);
  }
}
