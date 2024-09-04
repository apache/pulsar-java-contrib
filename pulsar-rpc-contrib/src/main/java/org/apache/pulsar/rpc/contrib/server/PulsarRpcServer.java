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

import static lombok.AccessLevel.PACKAGE;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.rpc.contrib.common.MessageDispatcherFactory;

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
@RequiredArgsConstructor(access = PACKAGE)
public class PulsarRpcServer<T, V> implements AutoCloseable {
    private final Consumer<?> requestConsumer;
    private final GenericKeyedObjectPool<?, ?> replyProducerPool;

    /**
     * Provides a builder to configure and create instances of {@link PulsarRpcServer}.
     *
     * @param requestSchema the schema for serializing and deserializing request messages
     * @param replySchema the schema for serializing and deserializing reply messages
     * @return a builder to configure and instantiate a {@link PulsarRpcServer}
     */
    public static <T, V> PulsarRpcServerBuilder<T, V> builder(
            @NonNull Schema<T> requestSchema, @NonNull Schema<V> replySchema) {
        return new PulsarRpcServerBuilder<>(requestSchema, replySchema);
    }

    /**
     * Creates a new instance of {@link PulsarRpcServer} using the specified Pulsar client and configuration.
     * It sets up the necessary consumer and producer resources according to the
     * provided {@link PulsarRpcServerBuilder} settings.
     *
     * @param pulsarClient the Pulsar client to use for connecting to the Pulsar instance
     * @param requestFunction the function to process incoming requests and produce replies
     * @param rollBackFunction the function to handle rollback in case of processing errors
     * @param builder the configuration builder with all necessary settings
     * @return a new instance of {@link PulsarRpcServer}
     * @throws IOException if there is an I/O error setting up Pulsar consumers or producers
     */
    public static <T, V> PulsarRpcServer<T, V> create(
            @NonNull PulsarClient pulsarClient,
            @NonNull Function<T, CompletableFuture<V>> requestFunction,
            @NonNull BiConsumer<String, T> rollBackFunction,
            @NonNull PulsarRpcServerBuilder<T, V> builder) throws IOException {
        MessageDispatcherFactory<T, V> dispatcherFactory = new MessageDispatcherFactory<>(
                pulsarClient,
                builder.getRequestSchema(),
                builder.getReplySchema(),
                builder.getRequestSubscription());
        ReplyProducerPoolFactory<V> poolFactory = new ReplyProducerPoolFactory<>(dispatcherFactory);
        GenericKeyedObjectPool<String, Producer<V>> pool = new GenericKeyedObjectPool<>(poolFactory);
        ReplySender<T, V> replySender = new ReplySender<>(pool, rollBackFunction);
        RequestListener<T, V> requestListener = new RequestListener<>(
                requestFunction,
                replySender,
                rollBackFunction);
        Consumer<T> requestConsumer = dispatcherFactory.requestConsumer(
                builder.getRequestTopic(),
                builder.getPatternAutoDiscoveryInterval(),
                requestListener,
                builder.getRequestTopicsPattern());

        return new PulsarRpcServer<>(
                requestConsumer,
                pool
        );
    }

    /**
     * Closes the RPC server, releasing all resources such as the request consumer and reply producer pool.
     * This method ensures that all underlying Pulsar clients are properly closed to free up network resources and
     * prevent memory leaks.
     *
     * @throws Exception if an error occurs during the closing of server resources
     */
    @Override
    public void close() throws Exception {
        requestConsumer.close();
        replyProducerPool.close();
    }
}
