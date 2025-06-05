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
import org.apache.pulsar.client.admin.PulsarAdmin;

public class OffsetToMessageIdCacheProvider {
    private static final Map<String, OffsetToMessageIdCache> CACHE_MAP = new ConcurrentHashMap<>();

    public static OffsetToMessageIdCache getOrCreateCache(PulsarAdmin pulsarAdmin, String brokerCluster) {
        return CACHE_MAP.computeIfAbsent(brokerCluster, key -> new OffsetToMessageIdCache(pulsarAdmin));
    }
}
