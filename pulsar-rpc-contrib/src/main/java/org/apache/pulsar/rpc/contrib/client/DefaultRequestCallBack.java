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

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

/**
 * Default implementation of {@link RequestCallBack} that handles callback events for Pulsar RPC communications.
 */
@Slf4j
public class DefaultRequestCallBack<V> implements RequestCallBack<V> {

    @Override
    public void onSendRequestSuccess(String correlationId, MessageId messageId) {

    }

    @Override
    public void onSendRequestError(String correlationId, Throwable t,
                                   CompletableFuture<V> replyFuture) {
        replyFuture.completeExceptionally(t);
    }

    @Override
    public void onReplySuccess(String correlationId, String subscription,
                               V value, CompletableFuture<V> replyFuture) {
        replyFuture.complete(value);
    }

    @Override
    public void onReplyError(String correlationId, String subscription,
                             String errorMessage, CompletableFuture<V> replyFuture) {
        replyFuture.completeExceptionally(new Exception(errorMessage));
    }

    @Override
    public void onTimeout(String correlationId, Throwable t) {

    }

    @Override
    public void onReplyMessageAckFailed(String correlationId, Consumer<V> consumer, Message<V> msg, Throwable t) {
        consumer.acknowledgeAsync(msg.getMessageId()).exceptionally(ex -> {
            log.warn("<onReplyMessageAckFailed> [{}] [{}] Acknowledging message {} failed again.",
                    msg.getTopicName(), correlationId, msg.getMessageId(), ex);
            return null;
        });
    }
}
