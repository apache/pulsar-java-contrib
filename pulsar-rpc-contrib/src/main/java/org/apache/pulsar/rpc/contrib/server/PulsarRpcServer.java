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

import lombok.NonNull;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.rpc.contrib.common.PulsarRpcServerException;

/**
 * Represents an RPC server that utilizes Apache Pulsar as the messaging layer to handle
 * request-response cycles in a distributed environment. This server is responsible for
 * receiving RPC requests, processing them, and sending the corresponding responses back
 * to the client.
 *
 * <p>This class integrates tightly with Apache Pulsar's consumer and producer APIs to
 * receive messages and send replies. It uses a {@link GenericKeyedObjectPool} to manage
 * a pool of Pulsar producers optimized for sending replies efficiently across different topics.</p>
 *
 * @param <T> the type of request message this server handles
 * @param <V> the type of response message this server sends
 */
public interface PulsarRpcServer<T, V> extends AutoCloseable {

    /**
     * Provides a builder to configure and create instances of {@link PulsarRpcServer}.
     *
     * @param requestSchema the schema for serializing and deserializing request messages
     * @param replySchema the schema for serializing and deserializing reply messages
     * @return a builder to configure and instantiate a {@link PulsarRpcServer}
     */
    static <T, V> PulsarRpcServerBuilder<T, V> builder(@NonNull Schema<T> requestSchema,
                                                       @NonNull Schema<V> replySchema) {
        return new PulsarRpcServerBuilderImpl<>(requestSchema, replySchema);
    }

    /**
     * Closes the RPC server, releasing all resources such as the request consumer and reply producer pool.
     * This method ensures that all underlying Pulsar clients are properly closed to free up network resources and
     * prevent memory leaks.
     *
     * @throws PulsarRpcServerException if an error occurs during the closing of server resources
     */
    @Override
    void close() throws PulsarRpcServerException;
}
