package org.apache.pulsar.client.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

public class ReaderCacheProvider {
    private static final Map<String, Map<Schema<?>, ReaderCache<?>>> CACHE_MAP = new ConcurrentHashMap<>();

    public static <T> ReaderCache<T> getOrCreateReaderCache(String brokerCluster, Schema<T> schema, PulsarClient client,
                                                    OffsetToMessageIdCache offsetToMessageIdCache) {
        Map<Schema<?>, ReaderCache<?>> partitionReaderCache = CACHE_MAP.computeIfAbsent(brokerCluster,
                key -> new ConcurrentHashMap<>());
        @SuppressWarnings("unchecked")
        ReaderCache<T> cache = (ReaderCache<T>) partitionReaderCache.computeIfAbsent(schema,
                key -> new ReaderCache<>(client, offsetToMessageIdCache, schema));
        return cache;
    }
}
