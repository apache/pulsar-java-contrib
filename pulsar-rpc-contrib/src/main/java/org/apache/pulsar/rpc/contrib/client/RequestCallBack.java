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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;

/**
 * Provides callback methods for various asynchronous events in Pulsar RPC communications. This
 * interface is used to define custom behaviors that occur in response to different stages of
 * message handling, such as request message successful send, send error, successful reply from
 * server, reply error from server, timeouts, and errors in reply message acknowledgment.
 *
 * <p>Implementations of this interface can be used to handle callbacks in a way that integrates
 * seamlessly with business logic, including error handling, logging, or retry mechanisms.
 *
 * @param <V> the type of reply message
 */
public interface RequestCallBack<V> {

  /**
   * Invoked after successfully sending a request to the server.
   *
   * @param correlationId A unique identifier for the request to correlate the response.
   * @param messageId The message ID of the request message sent to server.
   */
  void onSendRequestSuccess(String correlationId, MessageId messageId);

  /**
   * Invoked when an error occurs during the sending of a request message.
   *
   * <p>Please note that {@code replyFuture.completeExceptionally(t)} must be executed at the end.
   *
   * @param correlationId The correlation ID of the request.
   * @param t The throwable error that occurred during sending.
   * @param replyFuture The future where the error will be reported.
   */
  void onSendRequestError(String correlationId, Throwable t, CompletableFuture<V> replyFuture);

  /**
   * Invoked after receiving a reply from the server successfully.
   *
   * <p>Please note that {@code replyFuture.complete(value)} must be executed at the end.
   *
   * @param correlationId The correlation ID associated with the reply.
   * @param subscription The subscription name the reply was received on.
   * @param value The value of the reply.
   * @param replyFuture The future to be completed with the received value.
   */
  void onReplySuccess(
      String correlationId, String subscription, V value, CompletableFuture<V> replyFuture);

  /**
   * Invoked when an error occurs upon receiving a reply from the server.
   *
   * <p>Please note that {@code replyFuture.completeExceptionally(new Exception(errorMessage))} must
   * be executed at the end.
   *
   * @param correlationId The correlation ID of the request.
   * @param subscription The subscription name the error occurred on.
   * @param errorMessage The error message associated with the reply.
   * @param replyFuture The future to be completed exceptionally due to the error.
   */
  void onReplyError(
      String correlationId,
      String subscription,
      String errorMessage,
      CompletableFuture<V> replyFuture);

  /**
   * Invoked when receive reply message times out.
   *
   * @param correlationId The correlation ID associated with the request that timed out.
   * @param t The timeout exception or relevant throwable.
   */
  void onTimeout(String correlationId, Throwable t);

  /**
   * Invoked when acknowledging reply message fails.
   *
   * <p>You can retry or record the messageId of the reply message for subsequent processing
   * separately.
   *
   * <p>This piece does not affect the current function. Because the reply message has been
   * processed by onReplySuccess or onReplyError. When the user-defined request success condition is
   * met, the user removes the request through the removeRequest method of rpc client. Even if you
   * receive the reply message corresponding to this request in the future. But if there is no
   * request in the pendingRequestMap, it will not be processed.
   *
   * @param correlationId The correlation ID of the message.
   * @param consumer The consumer that is acknowledging the message.
   * @param msg The message that failed to be acknowledged.
   * @param t The throwable error encountered during acknowledgment.
   */
  void onReplyMessageAckFailed(
      String correlationId, Consumer<V> consumer, Message<V> msg, Throwable t);
}
