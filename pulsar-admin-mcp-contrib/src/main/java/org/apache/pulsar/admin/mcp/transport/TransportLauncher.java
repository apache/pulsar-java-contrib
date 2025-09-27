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

import org.apache.pulsar.admin.mcp.client.PulsarClientManager;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;

public class TransportLauncher {

    public static void start(PulsarMCPCliOptions options)  throws Exception {
        startTransport(options);
    }

    private static void startTransport(PulsarMCPCliOptions options) throws Exception {
        TransportManager transportManager = new TransportManager();

        StdioMCPServer stdio = new StdioMCPServer();
        HttpMCPServer http = new HttpMCPServer();

        PulsarClientManager manager = new PulsarClientManager();
        manager.initialize();
        stdio.injectClientManager(manager);
        http.injectClientManager(manager);

        transportManager.registerTransport(stdio);
        transportManager.registerTransport(http);

        final PulsarMCPCliOptions.TransportType chosen = options.getTransport();
        final Transport[] started = new Transport[1];


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (started[0] != null) {
                try {
                    started[0].stop();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                manager.close();
            } catch (Exception ignore) {

            }
        }, "pulsar-manager-shutdown"));

        switch (chosen) {
            case HTTP -> {
                transportManager.startTransport(PulsarMCPCliOptions.TransportType.HTTP, options);
                started[0] = http;
            }
            case STDIO -> {
                transportManager.startTransport(PulsarMCPCliOptions.TransportType.STDIO, options);
                started[0] = stdio;
            }
        }
    }
}
