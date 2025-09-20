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
import com.fasterxml.jackson.databind.SerializationFeature;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.admin.mcp.client.PulsarClientManager;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpMCPServer extends AbstractMCPServer implements Transport {

    private static final Logger logger = LoggerFactory.getLogger(HttpMCPServer.class);

    private final AtomicBoolean running = new AtomicBoolean(false);
    private Server jettyServer;

    public HttpMCPServer() {
        super();
    }

    @Override
    public void start(PulsarMCPCliOptions options) throws Exception {
        if (!running.compareAndSet(false, true)) {
            logger.warn("Server is already running");
            return;
        }
        try {
            if (this.pulsarClientManager == null) {
                running.set(false);
                throw new IllegalStateException("PulsarClientManager not injected.");
            }
            try {
                pulsarAdmin = pulsarClientManager.getAdmin();
            } catch (Exception e) {
                running.set(false);
                throw new RuntimeException("Failed to obtain PulsarAdmin from PulsarClientManager", e);
            }
            logger.info("Starting HTTP Streaming Pulsar MCP server");

            ObjectMapper mapper = new ObjectMapper()
                    .findAndRegisterModules()
                    .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

            var streamingTransport = HttpServletStreamableServerTransportProvider
                    .builder()
                    .objectMapper(mapper)
                    .build();

            var mcpServer = McpServer.sync(streamingTransport)
                    .serverInfo("pulsar-admin-http-streaming", "1.0.0")
                    .capabilities(McpSchema.ServerCapabilities.builder()
                    .tools(true)
                    .build())
                    .build();

            startJettyServer(streamingTransport, options.getHttpPort());

            logger.info("HTTP Streaming Pulsar MCP server started "
                    + "at http://localhost:{}/mcp/stream", options.getHttpPort());

        } catch (Exception e) {
            running.set(false);
            logger.error("Failed to start HTTP streaming server", e);
            throw e;
        }
    }

    @Override
    public void stop() throws Exception {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        logger.info("Stopping HTTP Streaming Pulsar MCP server....");

        if (jettyServer != null) {
            try {
                if (jettyServer.isRunning()) {
                    jettyServer.stop();
                }
            } catch (Exception e) {
                logger.warn("Error stopping Jetty: {}", e.getMessage());
            }
        }

        // 不在此关闭 pulsarClientManager（避免影响其它传输共用的客户端）
        logger.info("HTTP Streaming Pulsar MCP server stopped");
    }

    @Override
    public PulsarMCPCliOptions.TransportType getType() {
        return PulsarMCPCliOptions.TransportType.HTTP;
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    private void startJettyServer(HttpServletStreamableServerTransportProvider
                                          streamingTransport, int httpPort) throws Exception {
        jettyServer = new Server(httpPort);

        var context = new ServletContextHandler();
        context.setContextPath("/");
        jettyServer.setHandler(context);

        ServletHolder servletHolder = new ServletHolder(streamingTransport);
        servletHolder.setAsyncSupported(true);

        context.addServlet(servletHolder, "/mcp");
        context.addServlet(servletHolder, "/mcp/*");
        context.addServlet(servletHolder, "/mcp/stream");
        context.addServlet(servletHolder, "/mcp/stream/*");

        jettyServer.start();
        logger.info("HTTP Streamable transport ready at http://localhost:{}/mcp/stream", httpPort);
    }


    public static void main(String[] args) {
        try {
            HttpMCPServer transport = new HttpMCPServer();
            PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);

            PulsarClientManager manager = new PulsarClientManager(options);
            manager.initialize();
            transport.injectClientManager(manager);

            transport.start(options);

            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("Error starting HTTP Streaming Pulsar MCP server: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
