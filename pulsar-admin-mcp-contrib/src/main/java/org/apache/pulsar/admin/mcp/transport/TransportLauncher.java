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

import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;

public class TransportLauncher {

    public static void start(PulsarMCPCliOptions options)  throws Exception {
        TransportManager transportManager = new TransportManager();

        transportManager.registerTransport(new StdioMCPServer());
        transportManager.registerTransport(new HttpMCPServer());

        switch (options.getTransport()) {
            case HTTP -> {
                transportManager.startTransport(PulsarMCPCliOptions.TransportType.HTTP, options);
            }
            case STDIO -> {
                transportManager.startTransport(PulsarMCPCliOptions.TransportType.STDIO, options);
            }
            case ALL -> {
                transportManager.startAllTransports(options);
            }
        }
    }

    private static void applyConfiguration(PulsarMCPCliOptions options) {
       System.setProperty("PULSAR_ADMIN_URL", options.getAdminUrl());
       System.setProperty("PULSAR_SERVICE_URL", options.getServiceUrl());
       System.setProperty("mcp.transport", options.getTransport().getValue());
       System.setProperty("mcp.http.port", String.valueOf(options.getHttpPort()));

       if (options.isDebug()){
           System.setProperty("mcp.debug", "true");
           System.setProperty("logging.level.org.apache.pulsar.admin.mcp", "DEBUG");
       }

       if (options.getAuthPlugin() != null) {
           System.setProperty("pulsar.auth.plugin", options.getAuthPlugin());
       }

       if (options.getAuthParams() != null) {
           System.setProperty("pulsar.auth.params", options.getAuthParams());
       }
    }

    private static void startTransport(PulsarMCPCliOptions options) throws Exception {

        TransportManager transportManager = new TransportManager();

        transportManager.registerTransport(new StdioMCPServer());
        transportManager.registerTransport(new HttpMCPServer());

        switch (options.getTransport()) {
            case HTTP -> {
                transportManager.startTransport(PulsarMCPCliOptions.TransportType.HTTP, options);
            }
            case STDIO -> {
                transportManager.startTransport(PulsarMCPCliOptions.TransportType.STDIO, options);
            }
            case ALL -> {
                transportManager.startAllTransports(options);
            }
        }
    }

}
