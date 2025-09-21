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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions.TransportType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportManager {

    private static final Logger logger = LoggerFactory.getLogger(TransportManager.class);

    private final Map<TransportType, Transport> transports = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "Transport-Manager");
        thread.setDaemon(true);
        return thread;
    });

    public void registerTransport(Transport transport) {
        TransportType type = transport.getType();
        transports.put(type, transport);
    }

    public void startTransport(TransportType type, PulsarMCPCliOptions  options) throws Exception {
        Transport transport = transports.get(type);
        if (transport == null) {
            throw new IllegalArgumentException("Transport not registered: " + type);
        }

        transport.start(options);
    }

    public void startAllTransports(PulsarMCPCliOptions  options) throws Exception {
        logger.info("Starting all transports simultaneously");

        Transport httpTransport = transports.get(TransportType.HTTP);
        CompletableFuture<Void> httpFuture = null;

        if (httpTransport != null) {
            httpFuture = CompletableFuture.runAsync(() -> {
                try {
                    httpTransport.start(options);
                } catch (Exception e) {
                    if (options.isDebug()) {
                        logger.error("Exception in transport", e);
                    }
                }
            }, executorService);
        }

        if (httpFuture != null) {
            Thread.sleep(2000);
        }

        Transport stdioTransport = transports.get(TransportType.STDIO);
        if (stdioTransport != null) {
            stdioTransport.start(options);
        } else {
            logger.warn("No stdio transport registered");
        }
    }

}
