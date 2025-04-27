package org.apache.pulsar.client.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.admin.PulsarAdmin;

public class OffsetToMessageIdCacheProvider {
    private static final Map<String, OffsetToMessageIdCache> CACHE_MAP = new ConcurrentHashMap<>();

    public static OffsetToMessageIdCache getOrCreateCache(PulsarAdmin pulsarAdmin, String brokerCluster) {
        return CACHE_MAP.computeIfAbsent(brokerCluster, key -> new OffsetToMessageIdCache(pulsarAdmin));
    }
}
