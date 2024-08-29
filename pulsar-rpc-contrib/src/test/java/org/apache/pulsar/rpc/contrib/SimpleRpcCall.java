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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.rpc.contrib.client.PulsarRpcClient;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.UUID.randomUUID;

@Slf4j
public class SimpleRpcCall {
    private Supplier<String> correlationIdSupplier = () -> randomUUID().toString();

    @Test
    public void testRpcCall() throws Exception {
        final String topicBase = "testRpcCall";
        final String requestTopic = topicBase + "-request";
        final String replyTopic = topicBase + "-reply";
        final String replySub = topicBase + "-1";
        Duration replyTimeout = Duration.ofSeconds(10);
        final String synchronousMessage = "SynchronousRequest1";
        final String asynchronousMessage = "AsynchronousRequest1";

        Schema<TestRequest> requestSchema = Schema.JSON(TestRequest.class);
        Schema<TestReply> replySchema = Schema.JSON(TestReply.class);

        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        ProducerBuilder<TestRequest> requestProducerBuilder = pulsarClient.newProducer(requestSchema)
                .topic(requestTopic)
                .enableBatching(false)
                .producerName("requestProducer");

        PulsarRpcClient<TestRequest, TestReply> rpcClient = PulsarRpcClient.builder(requestSchema, replySchema)
                .requestProducer(requestProducerBuilder)
                .replyTopic(replyTopic)
                .replySubscription(replySub)
                .replyTimeout(replyTimeout)
                .build(pulsarClient);
        Producer<TestRequest> requestProducer = rpcClient.getRequestProducer();

        String correlationId = correlationIdSupplier.get();
        TypedMessageBuilder<TestRequest> message = requestProducer.newMessage()
                .key(correlationId)
                .value(new TestRequest(synchronousMessage));

        // 同步
        TestReply reply = rpcClient.request(correlationId, message);

        // 异步
        rpcClient.requestAsync(correlationId, message).whenComplete((replyMessage, e) -> {
            if (e != null) {
                log.error("error", e);
            } else {
                log.info("Reply message: " + replyMessage);
            }
        });
    }

    public record TestRequest(String value) {
    }

    public record TestReply(String value) {
    }

}
