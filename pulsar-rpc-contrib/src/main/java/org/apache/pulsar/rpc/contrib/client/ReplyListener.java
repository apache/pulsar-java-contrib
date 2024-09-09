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

import static org.apache.pulsar.rpc.contrib.common.Constants.ERROR_MESSAGE;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;

/**
 * Implements the {@link MessageListener} interface to handle reply messages for RPC requests in a Pulsar environment.
 * This listener manages the lifecycle of reply messages corresponding to each request, facilitating asynchronous
 * communication patterns and error handling based on callback mechanisms.
 *
 * @param <V> The type of the message payload expected in the reply messages.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class ReplyListener<V> implements MessageListener<V> {
    private final ConcurrentHashMap<String, CompletableFuture<V>> pendingRequestsMap;
    private final RequestCallBack<V> callBack;

    /**
     * Handles the reception of messages from reply-topic. This method is called whenever a message is received
     * on the subscribed topic. It processes the message based on its correlation ID and manages successful or
     * erroneous consumption through callbacks.
     *
     * @param consumer The consumer that received the message. Provides context for the message such as subscription
     *                 and topic information.
     * @param msg The message received from the topic. Contains data including the payload and metadata like the
     *            correlation ID and potential error messages.
     */
    @Override
    public void received(Consumer<V> consumer, Message<V> msg) {
        String correlationId = msg.getKey();
        try {
            if (!pendingRequestsMap.containsKey(correlationId)) {
                log.warn("[{}] [{}] No pending request found for correlationId {}."
                                + " This may indicate the message has already been processed or timed out.",
                        consumer.getTopic(), consumer.getConsumerName(), correlationId);
            } else {
                CompletableFuture<V> future = pendingRequestsMap.get(correlationId);
                String errorMessage = msg.getProperty(ERROR_MESSAGE);
                if (errorMessage != null) {
                    callBack.onReplyError(correlationId, consumer.getSubscription(), errorMessage, future);
                } else {
                    callBack.onReplySuccess(correlationId, consumer.getSubscription(), msg.getValue(), future);
                }
            }
        } finally {
            consumer.acknowledgeAsync(msg).exceptionally(ex -> {
                log.warn("[{}] [{}] Acknowledging message {} failed", msg.getTopicName(), correlationId,
                        msg.getMessageId(), ex);
                callBack.onReplyMessageAckFailed(correlationId, consumer, msg, ex);
                return null;
            });
        }
    }
}
