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
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;


@Slf4j
@RequiredArgsConstructor
public class ReplySender<T, V> {
    private final KeyedObjectPool<String, Producer<V>> pool;
    private final BiConsumer<String, T> rollBackFunction;

    @SneakyThrows
    public void sendReply(String topic, String correlationId, V reply, T value, String sub) {
        onSend(topic, correlationId, msg -> msg.value(reply), value, sub);
    }

    @SneakyThrows
    public void sendErrorReply(String topic, String correlationId, String errorMessage, T value, String sub) {
        onSend(topic, correlationId, msg -> msg.property(ERROR_MESSAGE, errorMessage).value(null), value, sub);
    }

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
