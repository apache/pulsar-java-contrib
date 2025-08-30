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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletSseServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSseMCPServer extends AbstractMCPServer implements Transport {

    private static final Logger logger = LoggerFactory.getLogger(HttpSseMCPServer.class);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Server jettyServer;

    @Override
    public void start(PulsarMCPCliOptions options) throws Exception {
        if (running.get()){
            logger.warn("Server is already running");
            return;
        }
        logger.info("Starting HTTP SSE Pulsar MCP server");

        initializePulsarAdmin();

        var objectMapper = new ObjectMapper();
        var sseTransport = HttpServletSseServerTransportProvider.builder()
                .objectMapper(objectMapper)
                .build();

        var mcpServer = McpServer.sync(sseTransport)
                .serverInfo("pulsar-admin-http-sse", "1.0.0")
                .capabilities(McpSchema.ServerCapabilities.builder()
                    .tools(true)
                    .build())
                .build();

        registerFilteredTools(mcpServer, options);

        startJettyServer(sseTransport, options.getHttpPort());

        running.set(true);
        logger.info("HTTP SSE Pulsar MCP server started at http://localhost:{}/mcp/sse", options.getHttpPort());

    }

    private void registerFilteredTools(McpSyncServer mcpServer, PulsarMCPCliOptions options) {
        Set<String> allTools = getAllAvailableTools();
        Set<String> enableTools = options.getFilteredTools(allTools);

        if (options.isDebug()){
            logger.info("Enabling filtered tools: {}", enableTools);
        }

        registerToolsConditionally(mcpServer, enableTools);

        logger.info("Registered {} tools for HTTP SSE transport", enableTools.size());

    }

    @Override
    public void stop() throws Exception {
        if (!running.get()){
            return;
        }

        logger.info("Stopping HTTP SSE Pulsar MCP server....");
        running.set(false);

        if (jettyServer != null && jettyServer.isRunning()) {
            jettyServer.stop();
        }

        logger.info("HTTP SSE Pulsar MCP server stopped");
    }

    @Override
    public PulsarMCPCliOptions.TransportType getType() {
        return PulsarMCPCliOptions.TransportType.HTTP;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void startJettyServer(HttpServletSseServerTransportProvider sseTransport, int httpPort) throws Exception {
        jettyServer = new  Server(httpPort);
        var context = new ServletContextHandler();
        context.setContextPath("/");
        jettyServer.setHandler(context);

        var mcpServlet = new ServletHolder(sseTransport);
        context.addServlet(mcpServlet, "mcp/message");
        context.addServlet(mcpServlet, "mcp/sse");

        jettyServer.start();

        logger.info("HTTP SSE transport ready at http://localhost:{}/mcp/sse", httpPort);
        logger.info("Message endpoint at  http://localhost:{}/mcp/message", httpPort);

        jettyServer.join();
    }

    public static void main(String[] args) {
        try {
            HttpSseMCPServer transport = new HttpSseMCPServer();
            PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);
            transport.start(options);
        } catch (Exception e) {
            System.err.println("Error starting HTTP SSE Pulsar MCP server: {}" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }


}
