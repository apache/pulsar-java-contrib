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

import static org.apache.pulsar.rpc.contrib.common.Constants.REPLY_TOPIC;
import static org.apache.pulsar.rpc.contrib.common.Constants.REQUEST_TIMEOUT_MILLIS;

import java.util.concurrent.CompletableFuture;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * Handles the sending of request messages to a specified Pulsar topic. This class encapsulates the
 * details of setting message properties related to the reply handling and timeout management before
 * sending the messages asynchronously.
 *
 * @param <T> The type of the payload of the request messages that this sender will handle.
 */
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class RequestSender<T> {
  private final String replyTopic;

  /**
   * Sends a request message asynchronously to request-topic specified during this object's
   * construction. This method adds necessary properties to the message such as the reply topic and
   * the request timeout before sending it.
   *
   * @param message The {@link TypedMessageBuilder} for building the message to be sent, allowing
   *     additional properties to be set before dispatch.
   * @param millis The timeout in milliseconds after which the request should be considered failed
   *     if no reply is received.
   * @return A {@link CompletableFuture} that will complete with the {@link MessageId} of the sent
   *     message once it has been successfully dispatched or will complete exceptionally if the send
   *     fails.
   */
  CompletableFuture<MessageId> sendRequest(TypedMessageBuilder<T> message, long millis) {
    return message
        .property(REPLY_TOPIC, replyTopic)
        .property(REQUEST_TIMEOUT_MILLIS, String.valueOf(millis))
        .sendAsync();
  }
}
