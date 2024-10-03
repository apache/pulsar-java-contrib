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
/**
 * Provides the server-side components necessary for building a RPC system based on Apache Pulsar.
 * This package includes classes that handle server-side request processing and response dispatching,
 * leveraging Pulsar's messaging capabilities.
 *
 * <p>Key components include:
 * <ul>
 *     <li>{@link org.apache.pulsar.rpc.contrib.server.PulsarRpcServer} - The RPC client used for sending replies
 *     and processing requests.</li>
 *     <li>{@link org.apache.pulsar.rpc.contrib.server.PulsarRpcServerImpl} - PulsarRpcServer implementation classes.
 *     <li>{@link org.apache.pulsar.rpc.contrib.server.PulsarRpcServerBuilder} - Aids in constructing a
 *     {@link org.apache.pulsar.rpc.contrib.server.PulsarRpcServer} instance with customized settings.</li>
 *     <li>{@link org.apache.pulsar.rpc.contrib.server.PulsarRpcServerBuilderImpl} - PulsarRpcServerBuilder
 *     implementation classes.</li>
 *     <li>{@link org.apache.pulsar.rpc.contrib.server.RequestListener} - Implements the Pulsar
 *     {@link org.apache.pulsar.client.api.MessageListener} interface to process messages as RPC requests.</li>
 *     <li>{@link org.apache.pulsar.rpc.contrib.server.ReplySender} - Manages sending responses back to clients through
 *     Pulsar producers.</li>
 *     <li>{@link org.apache.pulsar.rpc.contrib.server.ReplyProducerPoolFactory} - Manages a pool of Pulsar producers
 *     for efficient response dispatching.</li>
 * </ul>
 *
 */
package org.apache.pulsar.rpc.contrib.server;
