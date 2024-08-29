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
package org.apache.pulsar.rpc.contrib.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.pulsar.rpc.contrib.common.Constants.ERROR_MESSAGE;

@Slf4j
@RequiredArgsConstructor
public class ReplyListener<REPLY> implements MessageListener<REPLY> {
    private final ConcurrentHashMap<String, CompletableFuture<REPLY>> pendingRequestsMap;

    @Override
    public void received(Consumer<REPLY> consumer, Message<REPLY> msg) {
        String correlationId = msg.getKey();
        try {
            if (!pendingRequestsMap.containsKey(correlationId)) {
                log.warn("[{}] [{}] No pending request found for correlationId {}."
                                + " This may indicate the message has already been processed or timed out.",
                        consumer.getTopic(), consumer.getConsumerName(), correlationId);
            } else {
                CompletableFuture<REPLY> future = pendingRequestsMap.get(correlationId);
                String errorMessage = msg.getProperty(ERROR_MESSAGE);
                if (errorMessage != null) {
                    future.completeExceptionally(new Exception(errorMessage));
                } else {
                    future.complete(msg.getValue());
                }
            }
        } finally {
            consumer.acknowledgeAsync(msg).exceptionally(ex -> {
                log.warn("[{}] [{}] Acknowledging message {} failed", msg.getTopicName(), correlationId,
                        msg.getMessageId(), ex);
                return null;
            });
        }
    }

}
