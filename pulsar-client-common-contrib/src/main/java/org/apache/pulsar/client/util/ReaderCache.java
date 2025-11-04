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
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ReaderCache provides thread-safe management of Pulsar Reader instances with the following features.
 * - Partition-level locking for concurrent access
 * - LRU eviction with size-based and access-time-based policies
 * - Automatic resource cleanup
 */
public class ReaderCache<T> {
    private static final Logger log = LoggerFactory.getLogger(ReaderCache.class);
    private static final int DEFAULT_MAX_CACHE_SIZE = 100;
    private static final Duration DEFAULT_EXPIRE_AFTER_ACCESS = Duration.ofMinutes(5);
    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 100;

    private final PulsarClient pulsarClient;
    private final OffsetToMessageIdCache offsetToMessageIdCache;
    private final Schema<T> schema;
    private final Map<String, ReentrantLock> partitionLocks = new ConcurrentHashMap<>();
    private final Map<String, LoadingCache<Long, Reader<T>>> readerCacheMap = new ConcurrentHashMap<>();

    private final int maxCacheSize;
    private final Duration expireAfterAccess;

    public ReaderCache(PulsarClient pulsarClient,
                       OffsetToMessageIdCache offsetToMessageIdCache,
                       Schema<T> schema) {
        this(pulsarClient, offsetToMessageIdCache, schema, DEFAULT_MAX_CACHE_SIZE, DEFAULT_EXPIRE_AFTER_ACCESS);
    }

    public ReaderCache(PulsarClient pulsarClient,
                       OffsetToMessageIdCache offsetToMessageIdCache,
                       Schema<T> schema,
                       int maxCacheSize,
                       Duration expireAfterAccess) {
        this.pulsarClient = Objects.requireNonNull(pulsarClient);
        this.offsetToMessageIdCache = Objects.requireNonNull(offsetToMessageIdCache);
        this.schema = Objects.requireNonNull(schema);
        this.maxCacheSize = maxCacheSize;
        this.expireAfterAccess = expireAfterAccess;
    }

    private LoadingCache<Long, Reader<T>> createCache(String partitionTopic) {
        return Caffeine.newBuilder()
                .maximumSize(maxCacheSize)
                .expireAfterAccess(expireAfterAccess)
                .removalListener((RemovalListener<Long, Reader<T>>) (key, reader, cause) -> {
                    // Do not close reader on explicit removal
                    if (reader != null && cause != RemovalCause.EXPLICIT) {
                        closeReaderSilently(reader);
                    }
                })
                .recordStats()
                .build(key -> createReaderWithRetry(partitionTopic, key));
    }

    private Reader<T> createReaderWithRetry(String partitionTopic, Long offset) throws PulsarClientException {
        int attempts = 0;
        while (attempts < MAX_RETRIES) {
            try {
                return pulsarClient.newReader(schema)
                        .startMessageId(offsetToMessageIdCache.getMessageIdByOffset(partitionTopic, offset))
                        .topic(partitionTopic)
                        .create();
            } catch (PulsarClientException e) {
                if (++attempts >= MAX_RETRIES) {
                    throw e;
                }
                handleRetry(partitionTopic, offset, attempts, e);
            }
        }
        throw new PulsarClientException("Failed to create reader after " + MAX_RETRIES + " attempts");
    }

    private void handleRetry(String partitionTopic, Long offset, int attempt, Exception e) {
        log.warn("Reader creation failed [Topic: {}][Offset: {}] Attempt {}/{}: {}",
                partitionTopic, offset, attempt, MAX_RETRIES, e.getMessage());
        try {
            Thread.sleep(RETRY_DELAY_MS * attempt);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted during retry", ie);
        }
    }

    public Reader<T> getReader(String partitionTopic, long offset) {
        final ReentrantLock lock = partitionLocks.computeIfAbsent(partitionTopic, k -> new ReentrantLock());
        lock.lock();
        try {
            LoadingCache<Long, Reader<T>> cache = readerCacheMap.computeIfAbsent(
                    partitionTopic,
                    pt -> createCache(pt)
            );
            Reader<T> reader = cache.get(offset);
            // Ensure exclusive use: remove from cache but don't close
            cache.invalidate(offset);
            return reader;
        } finally {
            lock.unlock();
        }
    }

    // Allows users to proactively return the reader
    public void releaseReader(String partitionTopic, long offset, Reader<T> reader) {
        readerCacheMap.computeIfPresent(partitionTopic, (pt, cache) -> {
            if (reader.isConnected()) {
                cache.put(offset, reader);
            }
            return cache;
        });
    }

    public void cleanup() {
        readerCacheMap.forEach((partition, cache) -> {
            cache.invalidateAll();
            cache.cleanUp();
        });
        readerCacheMap.clear();
        partitionLocks.clear();
    }

    public CacheStats getCacheStats(String partitionTopic) {
        LoadingCache<?, ?> cache = readerCacheMap.get(partitionTopic);
        return cache != null ? new CacheStats(cache.stats()) : null;
    }

    private void closeReaderSilently(Reader<T> reader) {
        try {
            if (reader != null && reader.isConnected()) {
                reader.close();
            }
        } catch (Exception e) {
            log.error("Error closing reader for topic {}", reader.getTopic(), e);
        }
    }

    public static class CacheStats {
        private final com.github.benmanes.caffeine.cache.stats.CacheStats stats;

        CacheStats(com.github.benmanes.caffeine.cache.stats.CacheStats stats) {
            this.stats = stats;
        }

        public long hitCount() {
            return stats.hitCount();
        }

        public long missCount() {
            return stats.missCount();
        }

        public long loadSuccessCount() {
            return stats.loadSuccessCount();
        }

        public long loadFailureCount() {
            return stats.loadFailureCount();
        }
    }
}
