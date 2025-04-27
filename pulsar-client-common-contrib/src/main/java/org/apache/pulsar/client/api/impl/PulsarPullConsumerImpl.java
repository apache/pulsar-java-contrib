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
package org.apache.pulsar.client.api.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarPullConsumer;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.common.ConsumeStats;
import org.apache.pulsar.client.common.PullRequest;
import org.apache.pulsar.client.common.PullResponse;
import org.apache.pulsar.client.util.OffsetToMessageIdCache;
import org.apache.pulsar.client.util.OffsetToMessageIdCacheProvider;
import org.apache.pulsar.client.util.PulsarAdminUtils;
import org.apache.pulsar.client.util.ReaderCache;
import org.apache.pulsar.client.util.ReaderCacheProvider;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarPullConsumerImpl<T> implements PulsarPullConsumer<T> {
    private static final Logger log = LoggerFactory.getLogger(PulsarPullConsumerImpl.class);
    private static final String PARTITION_SPLICER = "-partition-";
    private static final int DEFAULT_READ_TIMEOUT_MS = 30_000;

    private final String topic;
    private final String subscription;
    private final String brokerCluster;
    private final Schema<T> schema;
    private final Map<String, Consumer<T>> consumerMap;
    private final OffsetToMessageIdCache offsetToMessageIdCache;
    private final ReaderCache<T> readerCache;
    private final PulsarAdmin pulsarAdmin;
    private final Supplier<PulsarClient> pulsarClientSupplier;

    private volatile int partitionCount;

    public PulsarPullConsumerImpl(String topic,
                                  String subscription,
                                  String brokerCluster,
                                  Schema<T> schema,
                                  Supplier<PulsarClient> clientSupplier,
                                  PulsarAdmin admin) {
        this.topic = Objects.requireNonNull(topic, "Topic must not be null");
        this.subscription = Objects.requireNonNull(subscription, "Subscription must not be null");
        this.brokerCluster = Objects.requireNonNull(brokerCluster, "Broker cluster must not be null");
        this.schema = Objects.requireNonNull(schema, "Schema must not be null");
        this.pulsarClientSupplier = Objects.requireNonNull(clientSupplier, "PulsarClient must not be null");
        this.pulsarAdmin = Objects.requireNonNull(admin, "PulsarAdmin must not be null");
        this.consumerMap = new ConcurrentHashMap<>();
        this.offsetToMessageIdCache = OffsetToMessageIdCacheProvider.getOrCreateCache(admin, brokerCluster);
        this.readerCache =
                ReaderCacheProvider.getOrCreateReaderCache(brokerCluster, schema, clientSupplier.get(),
                        offsetToMessageIdCache);
    }

    @Override
    public void start() throws PulsarClientException {
        try {
            initializePartitions();
        } catch (PulsarAdminException e) {
            throw new PulsarClientException("Failed to initialize partitions", e);
        }
    }

    private void initializePartitions() throws PulsarAdminException, PulsarClientException {
        PartitionedTopicMetadata metadata = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
        this.partitionCount = metadata.partitions;

        if (partitionCount == 0) {
            subscribeToTopic(topic);
            return;
        }

        for (int i = 0; i < partitionCount; i++) {
            String partitionTopic = buildPartitionTopic(topic, i);
            subscribeToTopic(partitionTopic);
        }
    }

    private void subscribeToTopic(String topicName) throws PulsarClientException {
        Consumer<T> consumer = getPulsarClient().newConsumer(schema)
                .topic(topicName)
                .subscriptionName(subscription)
                .subscribe();
        consumerMap.put(topicName, consumer);
        log.debug("Subscribed to topic: {}", topicName);
    }

    private PulsarClient getPulsarClient() {
        return Objects.requireNonNull(pulsarClientSupplier.get(),
                "PulsarClient supplier returned null. Ensure PulsarClient is properly initialized.");
    }

    @Override
    public PullResponse<T> pull(PullRequest request) {
        validatePullParameters(request.getMaxMessages(), request.getMaxBytes());

        String partitionTopic = buildPartitionTopic(topic, request.getPartition());
        Reader<T> reader = null;
        Message<T> lastMessage = null;
        try {
            reader = readerCache.getReader(partitionTopic, request.getOffset());
            List<Message<T>> messages = readMessages(reader, request.getMaxMessages(), request.getMaxBytes(),
                    request.getTimeout());
            lastMessage = messages.isEmpty() ? null : messages.get(messages.size() - 1);
            return new PullResponse<>(reader.hasMessageAvailable(), messages);
        } catch (PulsarClientException e) {
            log.error("Failed to pull messages from topic {} at offset {}", partitionTopic, request.getOffset(), e);
            return new PullResponse<>(false, Collections.emptyList());
        } finally {
            if (reader != null) {
                releaseReader(partitionTopic, reader,
                        lastMessage != null ? lastMessage.getIndex().get() + 1 : request.getOffset());
            }
        }
    }

    private List<Message<T>> readMessages(Reader<T> reader,
                                          int maxMessages,
                                          int maxBytes,
                                          Duration timeout) {
        List<Message<T>> messages = new ArrayList<>(Math.min(maxMessages, 1024));
        int totalBytes = 0;
        long deadline = System.nanoTime() + timeout.toNanos();

        while (messages.size() < maxMessages && totalBytes < maxBytes) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                break;
            }

            try {
                Message<T> msg = reader.readNext(
                        (int) Math.min(TimeUnit.NANOSECONDS.toMillis(remaining), DEFAULT_READ_TIMEOUT_MS),
                        TimeUnit.MILLISECONDS
                );
                if (msg == null) {
                    break;
                }

                messages.add(msg);
                totalBytes += msg.getData().length;
            } catch (PulsarClientException e) {
                log.warn("Error reading message from {}", reader.getTopic(), e);
                break;
            }
        }
        return Collections.unmodifiableList(messages);
    }

    @Override
    public void ack(long offset, int partition) throws PulsarClientException {
        String partitionTopic = buildPartitionTopic(topic, partition);
        Consumer<T> consumer = consumerMap.get(partitionTopic);
        if (consumer == null) {
            throw new PulsarClientException("Consumer not found for partition: " + partition);
        }

        MessageId messageId = offsetToMessageIdCache.getMessageIdByOffset(partitionTopic, offset);
        if (messageId == null) {
            throw new PulsarClientException("MessageID not found for offset: " + offset);
        }
        consumer.acknowledgeCumulative(messageId);
    }

    @Override
    public long searchOffset(int partition, long timestamp) throws PulsarAdminException {
        String partitionTopic = buildPartitionTopic(topic, partition);
        return PulsarAdminUtils.searchOffset(partitionTopic, timestamp, brokerCluster, pulsarAdmin);
    }

    @Override
    public ConsumeStats getConsumeStats(int partition) throws PulsarAdminException {
        String partitionTopic = buildPartitionTopic(topic, partition);
        return PulsarAdminUtils.getConsumeStats(partitionTopic, partition, subscription, brokerCluster, pulsarAdmin);
    }

    @Override
    public void close() {
        closeResources();
    }

    private void closeResources() {
        consumerMap.values().forEach(consumer -> {
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close consumer for topic {}", consumer.getTopic(), e);
            }
        });

        try {
            offsetToMessageIdCache.cleanup();
        } catch (Exception e) {
            log.warn("Error cleaning offset cache", e);
        }
    }

    private void validatePullParameters(int maxMessages, int maxBytes) {
        if (maxMessages <= 0) {
            throw new IllegalArgumentException("maxMessages must be positive");
        }
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be positive");
        }
    }

    private String buildPartitionTopic(String baseTopic, int partition) {
        if (partitionCount > 0 && partition >= partitionCount) {
            throw new IllegalArgumentException(String.format(
                    "Invalid partition %d for topic %s with %d partitions",
                    partition, baseTopic, partitionCount
            ));
        }
        return partitionCount == 0 ? baseTopic : baseTopic + PARTITION_SPLICER + partition;
    }

    private void releaseReader(String topicPartition, Reader<T> reader, long nextOffset) {
        try {
            if (reader.isConnected()) {
                readerCache.releaseReader(topicPartition, nextOffset, reader);
            }
        } catch (Exception e) {
            log.warn("Error releasing reader for {}", topicPartition, e);
        }
    }
}
