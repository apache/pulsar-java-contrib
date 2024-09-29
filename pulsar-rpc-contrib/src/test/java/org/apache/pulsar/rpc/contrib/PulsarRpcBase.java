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

import static java.util.UUID.randomUUID;
import java.time.Duration;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import lombok.Setter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.rpc.contrib.client.PulsarRpcClient;
import org.apache.pulsar.rpc.contrib.server.PulsarRpcServer;

@Setter
public abstract class PulsarRpcBase {
    protected final Supplier<String> correlationIdSupplier = () -> randomUUID().toString();
    protected final String topicPrefix = "public/default/";
    protected String requestTopic;
    protected String replyTopic;
    Pattern requestTopicPattern;
    Pattern replyTopicPattern;
    protected String requestSubBase;
    protected String replySubBase;
    protected Duration replyTimeout = Duration.ofSeconds(3);
    protected final String synchronousMessage = "SynchronousRequest";
    protected final String asynchronousMessage = "AsynchronousRequest";
    protected final Schema<TestRequest> requestSchema = Schema.JSON(TestRequest.class);
    protected final Schema<TestReply> replySchema = Schema.JSON(TestReply.class);
    protected final int messageNum = 10;

    protected PulsarAdmin pulsarAdmin;
    protected PulsarClient pulsarClient;
    protected PulsarRpcClient<TestRequest, TestReply> rpcClient;
    protected PulsarRpcServer<TestRequest, TestReply> rpcServer;

    protected final void internalSetup() throws Exception {
        pulsarAdmin = SingletonPulsarContainer.createPulsarAdmin();
        pulsarClient = SingletonPulsarContainer.createPulsarClient();
    }

    protected final void internalCleanup() throws Exception {
        pulsarClient.close();
        pulsarAdmin.topics().deletePartitionedTopic(requestTopic);
        pulsarAdmin.topics().deletePartitionedTopic(replyTopic);
        pulsarAdmin.close();
        if (rpcServer != null) {
            rpcServer.close();
        }
        if (rpcClient != null) {
            rpcClient.close();
        }
    }

    protected void setupTopic(String topicBase) throws Exception {
        this.requestTopic = topicBase + "-request";
        this.replyTopic = topicBase + "-reply";
        this.requestTopicPattern = Pattern.compile(topicPrefix + requestTopic + "-.*");
        this.replyTopicPattern = Pattern.compile(topicPrefix + replyTopic + "-.*");
        this.requestSubBase = requestTopic + "-sub";
        this.replySubBase = replyTopic + "-sub";
        pulsarAdmin.topics().createPartitionedTopic(requestTopic, 10);
        pulsarAdmin.topics().createPartitionedTopic(replyTopic, 10);
    }

    public record TestRequest(String value) {
    }

    public record TestReply(String value) {
    }
}
