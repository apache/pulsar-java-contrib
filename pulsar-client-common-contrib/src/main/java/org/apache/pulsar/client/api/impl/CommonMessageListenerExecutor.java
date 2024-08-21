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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;

public class CommonMessageListenerExecutor implements MessageListenerExecutor {
  private final ExecutorService executorService;

  public CommonMessageListenerExecutor(int numThreads, final String subscriptionName) {
    this.executorService =
        new ThreadPoolExecutor(
            numThreads,
            numThreads,
            10000L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactory() {
              private final AtomicInteger threadId = new AtomicInteger(0);

              @Override
              public Thread newThread(Runnable r) {
                return new Thread(
                    r, subscriptionName + "-listener-executor-" + threadId.incrementAndGet());
              }
            });
  }

  @Override
  public void execute(Message<?> message, Runnable runnable) {
    this.executorService.execute(runnable);
  }
}
