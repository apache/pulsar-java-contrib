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

@RequiredArgsConstructor
public class ReplyProducerPoolFactory<V> extends BaseKeyedPooledObjectFactory<String, Producer<V>> {
    private final MessageDispatcherFactory<?, V> dispatcherFactory;

    @Override
    public Producer<V> create(String topic) {
        try {
            return dispatcherFactory.replyProducer(topic);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public PooledObject<Producer<V>> wrap(Producer<V> producer) {
        return new DefaultPooledObject<>(producer);
    }

    @Override
    public void destroyObject(String topic, PooledObject<Producer<V>> pooledObject) throws Exception {
        pooledObject.getObject().close();
    }
}
