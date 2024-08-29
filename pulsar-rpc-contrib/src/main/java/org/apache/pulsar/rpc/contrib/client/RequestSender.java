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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.concurrent.CompletableFuture;

import static org.apache.pulsar.rpc.contrib.common.Constants.REPLY_TOPIC;
import static org.apache.pulsar.rpc.contrib.common.Constants.REQUEST_TIMEOUT_MILLIS;

@Slf4j
@RequiredArgsConstructor
public class RequestSender<REQUEST> {
    private final String replyTopic;

    CompletableFuture<MessageId> sendRequest(TypedMessageBuilder<REQUEST> message, long millis) {
        return message.property(REPLY_TOPIC, replyTopic)
                .property(REQUEST_TIMEOUT_MILLIS, String.valueOf(millis))
                .sendAsync();
    }
}
