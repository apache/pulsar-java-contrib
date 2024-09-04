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
 * Client-side implementation of a distributed RPC framework using Apache Pulsar
 *
 * <p>This package contains classes that are responsible for establishing and managing RPC interactions with the
 * Pulsar service.
 * It includes facilities for sending requests, receiving responses, and processing related callbacks.
 * These classes allow users to easily implement the RPC communication model, supporting both asynchronous and
 * synchronous message handling.
 *
 * <p>Key classes and interfaces include:
 * <ul>
 *   <li>{@link org.apache.pulsar.rpc.contrib.client.PulsarRpcClient} - The RPC client used for sending requests
 *   and processing responses.
 *   <li>{@link org.apache.pulsar.rpc.contrib.client.RequestSender} - A class for sending requests to the Pulsar server.
 *   <li>{@link org.apache.pulsar.rpc.contrib.client.RequestCallBack} - Defines callback methods to handle
 *   successful request transmissions, errors, and response successes or failures.
 *   <li>{@link org.apache.pulsar.rpc.contrib.client.ReplyListener} - A message listener that receives and
 *   processes reply message from the server.
 * </ul>
 *
 * <p>The implementation in this package relies on the Apache Pulsar client library.
 */
package org.apache.pulsar.rpc.contrib.client;
