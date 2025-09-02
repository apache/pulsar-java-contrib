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
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StdioMCPServer extends AbstractMCPServer implements Transport {

    private static final Logger logger = LoggerFactory.getLogger(StdioMCPServer.class);
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Override
    public void start(PulsarMCPCliOptions options) throws Exception {
        if (running.get()) {
            logger.warn("Stdio transport is already running");
            return;
        }

//        logger.info("Starting Pulsar MCP server");

        if (!options.isDebug()) {
            disableLogging();
        }

        try {
            initializePulsarAdmin();
        } catch (Exception e) {
            logger.error("Failed to initialize PulsarAdmin", e);
            if (options.isDebug()) {
                e.printStackTrace(System.err);
            }
            throw new RuntimeException("Cannot start MCP server without Pulsar connection. "
                    + "Please ensure Pulsar is running at"
                    + System.getProperty("PULSAR_ADMIN_URL", "http://localhost:8080"), e);
        }


        var mcpServer = McpServer.sync(new StdioServerTransportProvider())
                .serverInfo("pulsar-admin-stdio", "1.0.0")
                .capabilities(McpSchema.ServerCapabilities.builder()
                        .tools(true)
                        .build())
                .build();

        registerFilteredTools(mcpServer,  options);

        running.set(true);
//        logger.info("Pulsar MCP server started");

        Thread.currentThread().join();
    }

    @Override
    public void stop() throws Exception {
        if (!running.get()) {
            return;
        }

        logger.info("Stopping Pulsar MCP server....");
        running.set(false);
        logger.info("Pulsar MCP server stopped successfully");
    }

    @Override
    public PulsarMCPCliOptions.TransportType getType() {
        return PulsarMCPCliOptions.TransportType.STDIO;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void registerFilteredTools(McpSyncServer mcpServer, PulsarMCPCliOptions options) {

        Set<String> allTools = getAllAvailableTools();

        Set<String> enabledTools = options.getFilteredTools(allTools);

        if (options.isDebug()) {
            logger.info("Enable tools: {}", enabledTools);
        }

        registerToolsConditionally(mcpServer, enabledTools);

    }

    public static void main(String[] args) {
        try {
            StdioMCPServer server = new StdioMCPServer();
            PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);
            server.start(options);
        } catch (Exception e) {
            System.err.println("Error starting Pulsar MCP server: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
