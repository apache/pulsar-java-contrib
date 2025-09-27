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

import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StdioMCPServer extends AbstractMCPServer implements Transport {

    private static final Logger logger = LoggerFactory.getLogger(StdioMCPServer.class);
    private final AtomicBoolean running = new AtomicBoolean(false);

    public StdioMCPServer() {
        super();
    }

    @Override
    public void start(PulsarMCPCliOptions options) {
        if (!running.compareAndSet(false, true)) {
            logger.warn("Stdio transport is already running");
            return;
        }

        if (this.pulsarClientManager == null) {
            running.set(false);
            throw new IllegalStateException("PulsarClientManager not injected.");
        }

        try {
            pulsarAdmin = pulsarClientManager.getAdmin();
            pulsarClient = pulsarClientManager.getClient();
        } catch (Exception e) {
            running.set(false);
            logger.error("Failed to obtain Pulsar admin/client", e);
            throw new RuntimeException(
                    "Cannot start MCP server without Pulsar connection. Admin: "
                            + System.getProperty("PULSAR_ADMIN_URL", "http://localhost:8080"), e);
        }

        var mcpServer = McpServer.sync(new StdioServerTransportProvider())
                .serverInfo("pulsar-admin-stdio", "1.0.0")
                .capabilities(McpSchema.ServerCapabilities.builder().tools(true).build())
                .build();

        registerAllTools(mcpServer);

    }


    @Override
    public void stop() {
        if (!running.get()) {
            return;
        }

        running.set(false);

        if (pulsarClientManager != null) {
            try {
                pulsarClientManager.close();
            } catch (Exception e) {
                logger.warn("Error closing PulsarManager: {}", e.getMessage());
            }
        }

        logger.info("Pulsar MCP server stopped successfully");
    }

    @Override
    public PulsarMCPCliOptions.TransportType getType() {
        return PulsarMCPCliOptions.TransportType.STDIO;
    }

    public static void main(String[] args) {
        try {
            StdioMCPServer server = new StdioMCPServer();
            PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);
            server.start(options);
        } catch (Exception e) {
            logger.error("Error starting Pulsar MCP server: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
}
