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
package org.apache.pulsar.client.util;

import java.util.Optional;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.common.ConsumeStats;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarAdminUtils {

  private static final Logger log = LoggerFactory.getLogger(PulsarAdminUtils.class);

  public static long searchOffset(
      String partitionTopic, long timestamp, String brokerCluster, PulsarAdmin pulsarAdmin)
      throws PulsarAdminException {
    MessageIdAdv messageId =
        (MessageIdAdv) pulsarAdmin.topics().getMessageIdByTimestamp(partitionTopic, timestamp);
    return extractMessageIndex(partitionTopic, messageId, brokerCluster, pulsarAdmin);
  }

  public static ConsumeStats getConsumeStats(
      String partitionTopic,
      int partition,
      String subscription,
      String brokerCluster,
      PulsarAdmin pulsarAdmin)
      throws PulsarAdminException {
    ConsumeStats consumeStats = new ConsumeStats(partitionTopic, subscription, -1L, -1L, -1L);

    PersistentTopicInternalStats internalStats =
        pulsarAdmin.topics().getInternalStats(partitionTopic);
    if (internalStats == null || internalStats.ledgers.isEmpty()) {
      return consumeStats;
    }

    String consumedPosition =
        internalStats.cursors.containsKey(subscription)
            ? internalStats.cursors.get(subscription).markDeletePosition
            : "-1:-1";
    MessageIdAdv consumedMessageId = parseMessageIdFromString(consumedPosition, partition);
    String maxPosition = internalStats.lastConfirmedEntry;
    MessageIdAdv maxMessageId = parseMessageIdFromString(maxPosition, partition);
    String minPosition = internalStats.ledgers.get(0).ledgerId + ":-1";
    MessageIdAdv minMessageId = parseMessageIdFromString(minPosition, partition);

    // Ensure consumedMessageId is not less than minMessageId
    if (consumedMessageId.compareTo(minMessageId) < 0) {
      consumedMessageId = minMessageId;
    }

    // Ensure consumedMessageId is not greater than maxMessageId (lastConfirmedEntry).
    // When a consumer has acked all messages, the markDeletePosition may advance to the
    // beginning of a newly rolled-over ledger that has no confirmed entries yet. Reading
    // such a position would trigger a server-side "LastConfirmedEntry x:y when reading
    // ledger z" error, so we clamp it to the last confirmed entry, which semantically
    // means "all messages have been consumed".
    if (consumedMessageId.compareTo(maxMessageId) > 0) {
      consumedMessageId = maxMessageId;
    }

    consumeStats.setLastConsumedOffset(
        extractMessageIndex(partitionTopic, consumedMessageId, brokerCluster, pulsarAdmin));
    consumeStats.setMaxOffset(
        extractMessageIndex(partitionTopic, maxMessageId, brokerCluster, pulsarAdmin));
    consumeStats.setMinOffset(
        extractMessageIndex(partitionTopic, minMessageId, brokerCluster, pulsarAdmin));
    return consumeStats;
  }

  // Common message processing logic
  private static long extractMessageIndex(
      String topic, MessageIdAdv messageId, String brokerCluster, PulsarAdmin pulsarAdmin)
      throws PulsarAdminException {
    if (messageId == null || messageId.getLedgerId() < 0) {
      return -1;
    }
    long ledgerId = messageId.getLedgerId();
    long entryId = messageId.getEntryId();
    if (ledgerId > 0 && entryId < 0) {
      long index =
          getMessageIndex(
              topic,
              new MessageIdImpl(ledgerId, 0, messageId.getPartitionIndex()),
              brokerCluster,
              pulsarAdmin);
      // Keep the "no readable message" sentinel (-1) intact instead of turning it into -2.
      return index < 0 ? index : index - 1L;
    } else {
      return getMessageIndex(topic, messageId, brokerCluster, pulsarAdmin);
    }
  }

  private static long getMessageIndex(
      String topic, MessageIdAdv messageId, String brokerCluster, PulsarAdmin pulsarAdmin)
      throws PulsarAdminException {
    Message<byte[]> message;
    try {
      message =
          pulsarAdmin
              .topics()
              .getMessageById(topic, messageId.getLedgerId(), messageId.getEntryId());
    } catch (PulsarAdminException.NotFoundException
        | PulsarAdminException.ServerSideErrorException e) {
      // Either the message/position does not exist (NotFoundException) or it points to a
      // ledger/entry that has no confirmed data yet (ServerSideErrorException, e.g.
      // "LastConfirmedEntry is x:y when reading ledger z"). Both are expected boundary
      // cases rather than real failures, so we degrade gracefully by returning -1
      // (no valid index) instead of breaking the whole consume-stats computation.
      log.warn(
          "No readable message at {} in topic {}, treat as no valid index. reason: {}",
          messageId,
          topic,
          e.getMessage());
      return -1L;
    }

    if (message == null) {
      // No readable message at this position, keep consistent with the ServerSideError
      // handling above and degrade gracefully by returning -1 (no valid index).
      log.warn("No message found for {} in topic {}, treat as no valid index.", messageId, topic);
      return -1L;
    }

    Optional<Long> indexOptional = message.getIndex();
    if (indexOptional.isPresent()) {
      long index = indexOptional.get();
      OffsetToMessageIdCacheProvider.getOrCreateCache(pulsarAdmin, brokerCluster)
          .putMessageIdByOffset(topic, index, messageId);
      return index;
    } else {
      // The message carries no index (e.g. produced by an old client without brokerEntryMetadata).
      // Degrade gracefully by returning -1 to stay consistent with the other branches.
      log.warn(
          "Message index not found for {} in topic {}, treat as no valid index.", messageId, topic);
      return -1L;
    }
  }

  private static long processMessageId(
      String topic, MessageIdAdv messageId, String brokerCluster, PulsarAdmin pulsarAdmin)
      throws PulsarAdminException {
    try {
      return extractMessageIndex(topic, messageId, brokerCluster, pulsarAdmin);
    } catch (NumberFormatException e) {
      throw new PulsarAdminException("Invalid ID components: " + messageId, e);
    }
  }

  private static MessageIdAdv parseMessageIdFromString(String messageIdStr, int partition)
      throws PulsarAdminException {
    String[] parts = messageIdStr.split(":");
    if (parts.length < 2) {
      throw new PulsarAdminException("Invalid message ID format: " + messageIdStr);
    }
    try {
      long ledgerId = Long.parseLong(parts[0]);
      long entryId = Long.parseLong(parts[1]);
      return new MessageIdImpl(ledgerId, entryId, partition);
    } catch (NumberFormatException e) {
      throw new PulsarAdminException("Invalid message ID components: " + messageIdStr, e);
    }
  }
}
