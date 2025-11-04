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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;

/**
 * Cache for mapping offsets to MessageIds with the following features.
 * - Per-partition caching with configurable size and expiration
 * - Thread-safe cache initialization and access
 * - Automatic cache cleanup
 */
public class OffsetToMessageIdCache {
    private static final int DEFAULT_MAX_CACHE_SIZE = 1000;
    private static final Duration DEFAULT_EXPIRE_AFTER_ACCESS = Duration.ofMinutes(5);

    private final PulsarAdmin pulsarAdmin;
    private final Map<String, LoadingCache<Long, MessageId>> partitionCaches = new ConcurrentHashMap<>();
    private final int maxCacheSize;
    private final Duration expireAfterAccess;

    public OffsetToMessageIdCache(PulsarAdmin pulsarAdmin) {
        this(pulsarAdmin, DEFAULT_MAX_CACHE_SIZE, DEFAULT_EXPIRE_AFTER_ACCESS);
    }

    public OffsetToMessageIdCache(PulsarAdmin pulsarAdmin,
                                  int maxCacheSize,
                                  Duration expireAfterAccess) {
        this.pulsarAdmin = Objects.requireNonNull(pulsarAdmin, "PulsarAdmin must not be null");
        this.maxCacheSize = validatePositive(maxCacheSize, "Cache size must be positive");
        this.expireAfterAccess = Objects.requireNonNull(expireAfterAccess,
                "Expire duration must not be null");
    }

    private LoadingCache<Long, MessageId> createCache(String partitionTopic) {
        return Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterAccess(expireAfterAccess)
                .recordStats()
                .build(offset -> loadMessageId(partitionTopic, offset));
    }

    private MessageId loadMessageId(String partitionTopic, Long offset) throws PulsarAdminException {
        if (offset < 0) {
            return MessageId.earliest;
        }
        return pulsarAdmin.topics().getMessageIdByIndex(partitionTopic, offset);
    }

    public MessageId getMessageIdByOffset(String partitionTopic, long offset) {
        return partitionCaches
                .computeIfAbsent(partitionTopic, this::createCache)
                .get(offset);
    }

    public void putMessageIdByOffset(String partitionTopic, long offset, MessageId messageId) {
        partitionCaches
                .computeIfAbsent(partitionTopic, this::createCache)
                .put(offset, messageId);
    }

    /**
     * Cleans up all cached entries and releases resources.
     */
    public void cleanup() {
        partitionCaches.values().forEach(cache -> {
            cache.invalidateAll();
            cache.cleanUp();
        });
        partitionCaches.clear();
    }

    /**
     * Removes cache for specific partition topic.
     * @param partitionTopic topic partition to remove
     */
    public void removePartitionCache(String partitionTopic) {
        LoadingCache<Long, MessageId> cache = partitionCaches.remove(partitionTopic);
        if (cache != null) {
            cache.invalidateAll();
            cache.cleanUp();
        }
    }

    private static int validatePositive(int value, String errorMessage) {
        if (value <= 0) {
            throw new IllegalArgumentException(errorMessage);
        }
        return value;
    }
}
