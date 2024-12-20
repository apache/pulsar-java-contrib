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
 * This package provides common utilities and constants used across the Apache Pulsar RPC extensions.
 * It includes helper classes for managing RPC message flows and configurations.
 *
 * <p>Features include:</p>
 * <ul>
 *     <li>{@link org.apache.pulsar.rpc.contrib.common.Constants} - Constants for message properties and
 *     configurations.
 *     <li>{@link org.apache.pulsar.rpc.contrib.common.MessageDispatcherFactory} - Factory methods for configuring
 *     Pulsar producers and consumers tailored for RPC operations.
 *     <li>{@link org.apache.pulsar.rpc.contrib.common.PulsarRpcClientException} - Exception on PulsarRpc client side.
 *     <li>{@link org.apache.pulsar.rpc.contrib.common.PulsarRpcServerException} - Exception on PulsarRpc server side.
 * </ul>
 */
package org.apache.pulsar.rpc.contrib.common;
