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
package org.apache.pulsar.admin.mcp.transport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions.TransportType;

public class TransportManager {

  private final Map<TransportType, Transport> transports = new ConcurrentHashMap<>();

  public void registerTransport(Transport transport) {
    TransportType type = transport.getType();
    transports.put(type, transport);
  }

  public void startTransport(TransportType type, PulsarMCPCliOptions options) throws Exception {
    Transport transport = transports.get(type);
    if (transport == null) {
      throw new IllegalArgumentException("Transport not registered: " + type);
    }
    transport.start(options);
  }
}
