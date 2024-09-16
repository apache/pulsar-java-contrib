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
package org.apache.pulsar.rpc.contrib;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.rpc.contrib.server.PulsarRpcServer;
import org.apache.pulsar.rpc.contrib.server.PulsarRpcServerBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class PulsarRpcServerTest extends PulsarRpcBase {

    @BeforeMethod(alwaysRun = true)
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testPulsarRpcServer() throws Exception {
        setupTopic("testPulsarRpcServer");
        final int defaultEpoch = 1;
        AtomicInteger epoch = new AtomicInteger(defaultEpoch);

        // What do we do when we receive the request message
        Function<TestRequest, CompletableFuture<TestReply>> requestFunction = request -> {
            epoch.getAndIncrement();
            return CompletableFuture.completedFuture(new TestReply(request.value() + "-----------done"));
        };

        // If the server side is stateful, an error occurs after the server side executes 3-1, and a mechanism for
        // checking and rolling back needs to be provided.
        BiConsumer<String, TestRequest> rollbackFunction = (id, request) -> {
            if (epoch.get() != defaultEpoch) {
                epoch.set(defaultEpoch);
            }
        };

        PulsarRpcServerBuilder<TestRequest, TestReply> rpcServerBuilder =
                PulsarRpcServer.builder(requestSchema, replySchema)
                        .requestSubscription(requestSubBase)
                        .patternAutoDiscoveryInterval(Duration.ofSeconds(1));
        rpcServerBuilder.requestTopic(requestTopic);
        rpcServer = rpcServerBuilder.build(pulsarClient, requestFunction, rollbackFunction);
        Thread.sleep(3000);
    }

    @Test
    public void testPulsarRpcServerWithPattern() throws Exception {
        setupTopic("pattern");
        final int defaultEpoch = 1;
        AtomicInteger epoch = new AtomicInteger(defaultEpoch);

        // What do we do when we receive the request message
        Function<TestRequest, CompletableFuture<TestReply>> requestFunction = request -> {
            epoch.getAndIncrement();
            return CompletableFuture.completedFuture(new TestReply(request.value() + "-----------done"));
        };

        // If the server side is stateful, an error occurs after the server side executes 3-1, and a mechanism for
        // checking and rolling back needs to be provided.
        BiConsumer<String, TestRequest> rollbackFunction = (id, request) -> {
            if (epoch.get() != defaultEpoch) {
                epoch.set(defaultEpoch);
            }
        };

        PulsarRpcServerBuilder<TestRequest, TestReply> rpcServerBuilder =
                PulsarRpcServer.builder(requestSchema, replySchema)
                        .requestSubscription(requestSubBase)
                        .patternAutoDiscoveryInterval(Duration.ofSeconds(1));
        rpcServerBuilder.requestTopicsPattern(requestTopicPattern);
        rpcServer = rpcServerBuilder.build(pulsarClient, requestFunction, rollbackFunction);
        Thread.sleep(3000);
    }
}
