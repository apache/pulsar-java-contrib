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

import java.io.IOException;
import java.io.UncheckedIOException;
import lombok.RequiredArgsConstructor;
import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.rpc.contrib.common.MessageDispatcherFactory;

/**
 * A factory for creating and managing pooled {@link Producer} instances associated with specific topics.
 * This factory extends {@link BaseKeyedPooledObjectFactory}, customizing the creation, wrapping, and destruction
 * of Pulsar {@link Producer} instances for use in a {@link org.apache.commons.pool2.KeyedObjectPool}.
 *
 * <p>This factory leverages the {@link MessageDispatcherFactory} to create {@link Producer} instances,
 * ensuring that each producer is correctly configured according to the dispatcher settings.</p>
 *
 * @param <V> the type of messages the producers will send
 */
@RequiredArgsConstructor
public class ReplyProducerPoolFactory<V> extends BaseKeyedPooledObjectFactory<String, Producer<V>> {
    private final MessageDispatcherFactory<?, V> dispatcherFactory;

    /**
     * Creates a new {@link Producer} for the specified topic. This method is called internally by the pool
     * when a new producer is needed and not available in the pool.
     *
     * @param topic The topic for which the producer is to be created.
     * @return A new {@link Producer} instance configured for the specified topic.
     * @throws UncheckedIOException if an I/O error occurs when creating the producer.
     */
    @Override
    public Producer<V> create(String topic) {
        try {
            return dispatcherFactory.replyProducer(topic);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Wraps a {@link Producer} instance inside a {@link PooledObject} to manage pool operations such as
     * borrow and return.
     *
     * @param producer The {@link Producer} instance to wrap.
     * @return The {@link PooledObject} wrapping the provided {@link Producer}.
     */
    @Override
    public PooledObject<Producer<V>> wrap(Producer<V> producer) {
        return new DefaultPooledObject<>(producer);
    }

    /**
     * Destroys a {@link Producer} instance when it is no longer needed by the pool, ensuring that
     * resources are released and the producer is properly closed.
     *
     * @param topic The topic associated with the producer to be destroyed.
     * @param pooledObject The pooled object wrapping the producer that needs to be destroyed.
     * @throws Exception if an error occurs during the closing of the producer.
     */
    @Override
    public void destroyObject(String topic, PooledObject<Producer<V>> pooledObject) throws Exception {
        pooledObject.getObject().close();
    }
}
