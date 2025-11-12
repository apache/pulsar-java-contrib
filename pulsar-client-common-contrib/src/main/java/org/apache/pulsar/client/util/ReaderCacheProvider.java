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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

public class ReaderCacheProvider {
  private static final Map<String, Map<Schema<?>, ReaderCache<?>>> CACHE_MAP =
      new ConcurrentHashMap<>();

  public static <T> ReaderCache<T> getOrCreateReaderCache(
      String subscription,
      String brokerCluster,
      Schema<T> schema,
      PulsarClient client,
      OffsetToMessageIdCache offsetToMessageIdCache) {
    Map<Schema<?>, ReaderCache<?>> partitionReaderCache =
        CACHE_MAP.computeIfAbsent(brokerCluster, key -> new ConcurrentHashMap<>());
    @SuppressWarnings("unchecked")
    ReaderCache<T> cache =
        (ReaderCache<T>)
            partitionReaderCache.computeIfAbsent(
                schema,
                key -> new ReaderCache<>(subscription, client, offsetToMessageIdCache, schema));
    return cache;
  }
}
