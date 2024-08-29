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
package org.apache.pulsar.rpc.contrib.client;

import lombok.Getter;
import lombok.NonNull;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.time.Duration;
import java.util.regex.Pattern;

@Getter
public class PulsarRpcClientBuilder<REQUEST, REPLY> {
    private final Schema<REQUEST> requestSchema;
    private final Schema<REPLY> replySchema;
    private ProducerBuilder<REQUEST> requestProducer;
    private String replyTopic;
    private String replySubscription;
    private Duration replyTimeout = Duration.ofSeconds(3);
    private Pattern replyTopicsPattern;

    public PulsarRpcClientBuilder(@NonNull Schema<REQUEST> requestSchema, @NonNull Schema<REPLY> replySchema) {
        this.requestSchema = requestSchema;
        this.replySchema = replySchema;
    }

    public PulsarRpcClientBuilder<REQUEST, REPLY> requestProducer(@NonNull ProducerBuilder<REQUEST> requestProducer) {
        this.requestProducer = requestProducer;
        return this;
    }

    public PulsarRpcClientBuilder<REQUEST, REPLY> replyTopic(@NonNull String replyTopic) {
        this.replyTopic = replyTopic;
        return this;
    }

    public PulsarRpcClientBuilder<REQUEST, REPLY> replyTopic(@NonNull Pattern replyTopicsPattern) {
        this.replyTopicsPattern = replyTopicsPattern;
        return this;
    }

    public PulsarRpcClientBuilder<REQUEST, REPLY> replySubscription(@NonNull String replySubscription) {
        this.replySubscription = replySubscription;
        return this;
    }

    public PulsarRpcClientBuilder<REQUEST, REPLY> replyTimeout(@NonNull Duration replyTimeout) {
        this.replyTimeout = replyTimeout;
        return this;
    }

    public PulsarRpcClient<REQUEST, REPLY> build(PulsarClient pulsarClient) throws IOException {
        return PulsarRpcClient.create(pulsarClient, this);
    }
}
