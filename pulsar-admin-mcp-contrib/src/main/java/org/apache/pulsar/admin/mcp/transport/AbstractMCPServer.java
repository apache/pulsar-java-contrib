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

import io.modelcontextprotocol.server.McpSyncServer;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.admin.mcp.client.PulsarClientManager;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.apache.pulsar.admin.mcp.tools.ClusterTools;
import org.apache.pulsar.admin.mcp.tools.MessageTools;
import org.apache.pulsar.admin.mcp.tools.MonitoringTools;
import org.apache.pulsar.admin.mcp.tools.NamespaceTools;
import org.apache.pulsar.admin.mcp.tools.SchemaTools;
import org.apache.pulsar.admin.mcp.tools.SubscriptionTools;
import org.apache.pulsar.admin.mcp.tools.TenantTools;
import org.apache.pulsar.admin.mcp.tools.TopicTools;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractMCPServer {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractMCPServer.class);
    protected PulsarClientManager pulsarClientManager;
    protected static PulsarAdmin pulsarAdmin;

    public void initializePulsarAdmin(PulsarMCPCliOptions options) throws Exception {
        String adminUrl = System.getenv().getOrDefault("PULSAR_ADMIN_URL", "http://localhost:8080");

        try {
            PulsarAdminBuilder adminBuilder = PulsarAdmin.builder()
                    .serviceHttpUrl(adminUrl)
                    .connectionTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(60, TimeUnit.SECONDS);

            String authPlugin = System.getProperty("pulsar.auth-plugin");
            String authParams = System.getProperty("pulsar.auth-params");
            if (authPlugin != null && authParams != null) {
                adminBuilder.authentication(authPlugin, authParams);
                LOGGER.info("Authentication configured: {}", authPlugin);
            }

            pulsarAdmin = adminBuilder.build();

            try {
                pulsarAdmin.clusters().getClusters();
            } catch (Exception e) {
                pulsarAdmin.close();
                pulsarAdmin = null;
                throw new RuntimeException("Cannot connect to Pulsar clusters.", e);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to initialize PulsarAdmin", e);
        }
    }

    protected PulsarAdmin getPulsarAdmin() throws Exception {
        return pulsarClientManager.getAdmin();
    }

    protected PulsarClient getPulsarClientManager() throws Exception {
        return pulsarClientManager.getClient();
    }

    protected static void disableLogging() {
        System.setProperty("slf4j.internal.verbosity", "WARN");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "OFF");
        System.setProperty("logging.level.root", "OFF");
        System.setProperty("logging.level.io.modelcontextprotocol", "OFF");
        System.setProperty("org.eclipse.jetty.LEVEL", "WARN");
    }

    protected void registerFilteredTools(McpSyncServer mcpServer, PulsarMCPCliOptions options) {
        Set<String> enabledTools = options.getFilteredTools(getAllAvailableTools());

        if (options.isDebug()) {
            LOGGER.info("Enabling tools: {}", enabledTools);
        }

        try {
            registerToolsConditionally(mcpServer, enabledTools, pulsarClientManager);
        } catch (Exception e) {
            throw new RuntimeException("Failed to register tools", e);
        }
    }

    protected  static void registerToolsConditionally(
            McpSyncServer mcpServer, Set<String> enabledTools,
            PulsarClientManager pulsarClientManager) {
        if (pulsarAdmin == null) {
            throw new RuntimeException("PulsarAdmin has not been initialized");
        }

        if (enabledTools.stream().anyMatch(tool -> tool.contains("cluster") || tool.contains("broker"))) {
            registerToolGroup("ClusterTools", () -> {
                var clusterTools = new ClusterTools(pulsarAdmin);
                clusterTools.registerTools(mcpServer);
            });
        }

        if (enabledTools.stream().anyMatch(tool -> tool.contains("topic"))) {
            registerToolGroup("TopicTools", () -> {
                var topicTools = new TopicTools(pulsarAdmin);
                topicTools.registerTools(mcpServer);
            });
        }

        if (enabledTools.stream().anyMatch(tool -> tool.contains("tenant"))) {
            registerToolGroup("TenantTools", () -> {
                var tenantTools = new TenantTools(pulsarAdmin);
                tenantTools.registerTools(mcpServer);
            });
        }

        if (enabledTools.stream().anyMatch(tool -> tool.contains("namespace")
                || tool.contains("retention") || tool.contains("backlog"))) {
            registerToolGroup("NamespaceTools", () -> {
                var namespaceTools = new NamespaceTools(pulsarAdmin);
                namespaceTools.registerTools(mcpServer);
            });
        }

        if (enabledTools.stream().anyMatch(tool -> tool.contains("schema"))) {
            registerToolGroup("SchemaTools", () -> {
                var schemaTools = new SchemaTools(pulsarAdmin);
                schemaTools.registerTools(mcpServer);
            });
        }

        if (enabledTools.stream().anyMatch(tool -> tool.contains("message"))) {
            registerToolGroup("MessageTools", () -> {
                var messageTools = new MessageTools(pulsarAdmin, pulsarClientManager);
                messageTools.registerTools(mcpServer);
            });
        }

        if (enabledTools.stream().anyMatch(tool -> tool.contains("subscription") || tool.contains("unsubscribe"))) {
            registerToolGroup("SubscriptionTools", () -> {
                var subscriptionTools = new SubscriptionTools(pulsarAdmin);
                subscriptionTools.registerTools(mcpServer);
            });
        }

        if (enabledTools.stream().anyMatch(tool -> tool.contains("monitor")
                || tool.contains("health") || tool.contains("backlog-analysis"))) {
            registerToolGroup("MonitoringTools", () -> {
                var monitoringTools = new MonitoringTools(pulsarAdmin);
                monitoringTools.registerTools(mcpServer);
            });
        }
    }

    private static void registerToolGroup(String toolGroupName, Runnable registrationTask) {
        try {
            registrationTask.run();
        } catch (NoClassDefFoundError e) {
            System.err.println(toolGroupName + "dependencies missing" + e.getMessage());
        } catch (Exception e) {
            if (e.getCause() instanceof ClassNotFoundException) {
                System.err.println(toolGroupName + "not available in this configuration (class not found)");
            } else {
                System.err.println(toolGroupName + "dependencies missing" + e.getMessage());
                if (Boolean.parseBoolean(System.getProperty("mcp.debug", "false"))) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected static Set<String> getAllAvailableTools() {
        return Set.of(
                "list-clusters",
                "get-cluster-info",
                "create-cluster",
                "update-cluster-config",
                "delete-cluster",
                "get-cluster-stats",
                "list-brokers",
                "get-broker-stats",
                "get-cluster-failure-domain",
                "set-cluster-failure-domain",

                "list-tenants",
                "get-tenant-info",
                "create-tenant",
                "update-tenant",
                "delete-tenant",
                "get-tenant-stats",

                "list-namespaces",
                "get-namespace-info",
                "create-namespace",
                "delete-namespace",
                "set-retention-policy",
                "get-retention-policy",
                "set-backlog-quota",
                "get-backlog-quota",
                "clear-namespace-backlog",
                "get-namespace-stats",

                "list-topics",
                "create-topic",
                "delete-topic",
                "get-topic-stats",
                "get-topic-metadata",
                "update-topic-partitions",
                "compact-topic",
                "unload-topic",
                "get-topic-backlog",
                "expire-topic-messages",
                "peek-messages",
                "reset-topic-cursor",
                "get-topic-internal-stats",
                "get-partitioned-metadata",

                "list-subscriptions",
                "create-subscription",
                "delete-subscription",
                "get-subscription-stats",
                "reset-subscription-cursor",
                "skip-messages",
                "expire-subscription-messages",
                "pause-subscription",
                "resume-subscription",
                "unsubscribe",

                "peek-topic-messages",
                "skip-all-messages",
                "expire-all-messages",
                "get-message-by-id",
                "get-message-backlog",
                "send-message",
                "get-message-stats",
                "examine-messages",

                "get-schema-info",
                "get-schema-version",
                "get-all-schema-versions",
                "upload-schema",
                "delete-schema",
                "test-schema-compatibility",

                "monitor-cluster-performance",
                "monitor-topic-performance",
                "monitor-subscription-performance",
                "health-check",
                "connection-diagnostics",
                "backlog-analysis"
        );
    }

}
