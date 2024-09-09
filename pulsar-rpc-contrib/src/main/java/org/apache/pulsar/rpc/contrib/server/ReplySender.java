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
package org.apache.pulsar.rpc.contrib.server;

import static org.apache.pulsar.rpc.contrib.common.Constants.ERROR_MESSAGE;
import static org.apache.pulsar.rpc.contrib.common.Constants.SERVER_SUB;
import java.util.function.BiConsumer;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * A utility class for sending reply messages back to clients or reporting errors encountered during
 * the processing of requests. This class manages a pool of {@link Producer} instances for sending messages.
 *
 * <p>The {@link ReplySender} utilizes a {@link KeyedObjectPool} to manage {@link Producer} instances
 * for different topics to optimize resource usage and manage producer lifecycle.</p>
 *
 * @param <T> The type of the request payload.
 * @param <V> The type of the reply payload.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class ReplySender<T, V> {
    private final KeyedObjectPool<String, Producer<V>> pool;
    private final BiConsumer<String, T> rollBackFunction;

    /**
     * Sends a reply message to a given topic with specified correlation ID and value.
     *
     * @param topic         The topic to which the reply is sent.
     * @param correlationId The unique identifier of the request to which this reply corresponds.
     * @param reply         The reply content to send.
     * @param value         The original value received with the request, used for rollback purposes if needed.
     * @param sub           The subscriber name involved in this interaction.
     */
    @SneakyThrows
    public void sendReply(String topic, String correlationId, V reply, T value, String sub) {
        onSend(topic, correlationId, msg -> msg.value(reply), value, sub);
    }

    /**
     * Sends an error reply to a given topic indicating that an error occurred during processing of the request.
     *
     * @param topic         The topic to which the error reply is sent.
     * @param correlationId The unique identifier of the request for which this error reply is sent.
     * @param errorMessage  The error message to include in the reply.
     * @param value         The original value received with the request, used for rollback purposes if needed.
     * @param sub           The subscriber name involved in this interaction.
     */
    @SneakyThrows
    public void sendErrorReply(String topic, String correlationId, String errorMessage, T value, String sub) {
        onSend(topic, correlationId, msg -> msg.property(ERROR_MESSAGE, errorMessage).value(null), value, sub);
    }

    /**
     * Internal method to handle the mechanics of sending replies or error messages.
     * It manages the acquisition and return of {@link Producer} instances from a pool.
     *
     * @param topic         The topic to which the message is sent.
     * @param correlationId The correlation ID associated with the message.
     * @param consumer      A consumer that sets the properties of the message to be sent.
     * @param value         The original value received with the request.
     * @param sub           The subscriber name to be included in the message metadata.
     */
    @SneakyThrows
    public void onSend(String topic,
                       String correlationId,
                       java.util.function.Consumer<TypedMessageBuilder<V>> consumer,
                       T value,
                       String sub) {
        log.debug("Sending {}", correlationId);
        Producer<V> producer = pool.borrowObject(topic);
        try {
            TypedMessageBuilder<V> builder = producer.newMessage()
                    .key(correlationId)
                    .property(SERVER_SUB, sub);
            consumer.accept(builder);
            builder.sendAsync()
                    .exceptionally(e -> {
                        log.error("Failed to send reply", e);
                        rollBackFunction.accept(correlationId, value);
                        return null;
                    });
        } finally {
            pool.returnObject(topic, producer);
        }
    }
}
