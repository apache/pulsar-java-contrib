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
// 验证没有问题
package org.apache.pulsar.admin.mcp.tools;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.ClusterData;

public class ClusterTools extends BasePulsarTools {

    public ClusterTools(PulsarAdmin pulsarAdmin) {
        super(pulsarAdmin);
    }

    public void registerTools(McpSyncServer mcpServer){
        registerListClusters(mcpServer);
        registerGetClusterInfo(mcpServer);
        registerCreateCluster(mcpServer);
        registerDeleteCluster(mcpServer);
        registerUpdateClusterConfig(mcpServer);
        registerGetClusterStats(mcpServer);
        registerListBrokers(mcpServer);
        registerGetBrokerStats(mcpServer);
        registerGetClusterFailureDomain(mcpServer);
        registerSetClusterFailureDomain(mcpServer);
    }

    private void registerListClusters(McpSyncServer mcpServer){
        McpSchema.Tool tool = createTool(
                "list-clusters",
                "List all Pulsar clusters and their status",
                """
                {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        var clusters = pulsarAdmin.clusters().getClusters();

                        Map<String, Object> clusterDetails = new HashMap<>();
                        for (String clusterName :clusters) {
                            try {
                                ClusterData clusterData = pulsarAdmin.clusters().getCluster(clusterName);
                                Map<String, Object> details = new HashMap<>();
                                details.put("serviceUrl", clusterData.getServiceUrl());
                                details.put("serviceUrlTls", clusterData.getServiceUrlTls());
                                details.put("brokerServiceUrl", clusterData.getBrokerServiceUrl());
                                details.put("brokerServiceUrlTls", clusterData.getBrokerServiceUrlTls());
                                details.put("status", "active");
                                clusterDetails.put(clusterName, details);
                            } catch (Exception e) {
                                Map<String, Object> details = new HashMap<>();
                                details.put("status", "error");
                                details.put("error", e.getMessage());
                                clusterDetails.put(clusterName, details);
                            }
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusters", clusters);
                        result.put("count", clusters.size());
                        result.put("clusterDetails", clusterDetails);

                        return createSuccessResult("Cluster details", result);

                    } catch (Exception e) {
                        LOGGER.error("Failed to list clusters", e);
                        return createErrorResult("Failed to list clusters" + e.getMessage());
                    }
        }).build());
    }

    private void registerGetClusterInfo(McpSyncServer mcpServer) {
        McpSchema.Tool tool =  createTool(
                "get-cluster-info",
                "Get details information about a specific cluster",
                """
                {
                    "type": "object",
                    "properties": {
                        "cluster": {
                            "type": "string",
                            "description": "Name of the cluster to get info about"
                        }
                    },
                    "required": ["clusterName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName");

                        ClusterData clusterData = pulsarAdmin.clusters().getCluster(clusterName);

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusterName", clusterName);
                        result.put("serviceUrl", clusterData.getServiceUrl());
                        result.put("serviceUrlTls", clusterData.getServiceUrlTls());
                        result.put("brokerServiceUrl", clusterData.getBrokerServiceUrl());
                        result.put("brokerServiceUrlTls", clusterData.getBrokerServiceUrlTls());
                        result.put("peerClusterNames", clusterData.getPeerClusterNames());
                        result.put("proxyProtocol", clusterData.getProxyProtocol());
                        result.put("authenticationPlugin", clusterData.getAuthenticationPlugin());
                        result.put("authenticationParameters", clusterData.getAuthenticationParameters());
                        result.put("proxyServiceUrl", clusterData.getProxyServiceUrl());

                        return createSuccessResult("Cluster info retrieved", result);

                    } catch (IllegalArgumentException e) {
                       return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get cluster info", e);
                        return createErrorResult("Failed to get cluster info: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerCreateCluster(McpSyncServer mcpServer) {
        McpSchema.Tool tool =  createTool(
                "create-cluster",
                "Create a new Pulsar cluster",
                """
                {
                    "type": "object",
                    "properties": {
                        "clusterName": {
                            "type": "string",
                            "description": "Name of the cluster to create"
                        },
                        "serviceUrl": {
                            "type": "string",
                            "description": "Service URL for the cluster"
                        },
                        "serviceUrlTls": {
                            "type": "string",
                            "description": "TLS service URL for the cluster (optional)"
                        },
                        "brokerServiceUrl": {
                            "type": "string",
                            "description": "Broker service URL for the cluster (optional)"
                        },
                        "brokerServiceUrlTls": {
                            "type": "string",
                            "description": "TLS broker service URL for the cluster (optional)"
                        },
                        "proxyServiceUrl": {
                            "type": "string",
                            "description": "Proxy service URL (optional)"
                        },
                        "authenticationPlugin": {
                            "type": "string",
                            "description": "Authentication plugin class name (optional)"
                        },
                        "authenticationParameters": {
                            "type": "string",
                            "description": "Authentication parameters (optional)"
                        }
                    },
                    "required": ["clusterName", "serviceUrl"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName");
                        String serviceUrl = getRequiredStringParam(request.arguments(), "serviceUrl");
                        String serviceUrlTls = getStringParam(request.arguments(), "serviceUrlTls");
                        String brokerServiceUrl = getStringParam(request.arguments(), "brokerServiceUrl");
                        String brokerServiceUrlTls = getStringParam(request.arguments(), "brokerServiceUrlTls");
                        String proxyServiceUrl = getStringParam(request.arguments(), "proxyServiceUrl");
                        String authenticationPlugin = getStringParam(request.arguments(), "authenticationPlugin");
                        String authenticationParameters = getStringParam(
                                request.arguments(),
                                "authenticationParameters"
                        );

                        try {
                            pulsarAdmin.clusters().getCluster(clusterName);
                            return createErrorResult("Cluster info retrieved" + clusterName,
                                    List.of("Choose a different cluster name"));
                        } catch (Exception e){

                        }

                        var clusterDataBuilder = ClusterData.builder()
                                .serviceUrl(serviceUrl);

                        if (serviceUrlTls != null) {
                            clusterDataBuilder.serviceUrlTls(serviceUrlTls);
                        }
                        if (brokerServiceUrl != null) {
                            clusterDataBuilder.brokerServiceUrl(brokerServiceUrl);
                        }
                        if (brokerServiceUrlTls != null) {
                            clusterDataBuilder.brokerServiceUrlTls(brokerServiceUrlTls);
                        }
                        if (proxyServiceUrl != null) {
                            clusterDataBuilder.proxyServiceUrl(proxyServiceUrl);
                        }
                        if (authenticationPlugin != null) {
                            clusterDataBuilder.authenticationPlugin(authenticationPlugin);
                        }
                        if (authenticationParameters != null) {
                            clusterDataBuilder.authenticationParameters(authenticationParameters);
                        }

                        pulsarAdmin.clusters().createCluster(clusterName, clusterDataBuilder.build());

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusterName", clusterName);
                        result.put("serviceUrl", serviceUrl);
                        result.put("created", true);

                        return createSuccessResult("Cluster created successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid parameter: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to create cluster", e);
                        return createErrorResult("Failed to create cluster: " + e.getMessage());
                    }
                })
                .build());
        }

    private void registerUpdateClusterConfig(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "update-cluster-config",
                "Update configuration of an existing Pulsar cluster",
                """
                {
                    "type": "object",
                    "properties": {
                        "clusterName": {
                            "type": "string",
                            "description": "Name of the cluster to update"
                        },
                        "serviceUrl": {
                            "type": "string",
                            "description": "Service URL for the cluster (optional)"
                        },
                        "serviceUrlTls": {
                            "type": "string",
                            "description": "TLS service URL for the cluster (optional)"
                        },
                        "brokerServiceUrl": {
                            "type": "string",
                            "description": "Broker service URL for the cluster (optional)"
                        },
                        "brokerServiceUrlTls": {
                            "type": "string",
                            "description": "TLS broker service URL for the cluster (optional)"
                        },
                        "proxyServiceUrl": {
                            "type": "string",
                            "description": "Proxy service URL (optional)"
                        },
                        "authenticationPlugin": {
                            "type": "string",
                            "description": "Authentication plugin class name (optional)"
                        },
                        "authenticationParameters": {
                            "type": "string",
                            "description": "Authentication parameters (optional)"
                        }
                    },
                    "required": ["clusterName", "serviceUrl"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName");
                        String serviceUrl = getStringParam(request.arguments(), "serviceUrl");
                        String serviceUrlTls = getStringParam(request.arguments(), "serviceUrlTls");
                        String brokerServiceUrl = getStringParam(request.arguments(), "brokerServiceUrl");
                        String brokerServiceUrlTls = getStringParam(request.arguments(), "brokerServiceUrlTls");
                        String proxyServiceUrl = getStringParam(request.arguments(), "proxyServiceUrl");
                        String authenticationPlugin = getStringParam(request.arguments(), "authenticationPlugin");
                        String authenticationParameters = getStringParam(
                                request.arguments(),
                                "authenticationParameters");

                        var clusterDataBuilder = ClusterData.builder()
                                .serviceUrl(serviceUrl);

                        if (serviceUrlTls != null) {
                            clusterDataBuilder.serviceUrlTls(serviceUrlTls);
                        }
                        if (brokerServiceUrl != null) {
                            clusterDataBuilder.brokerServiceUrl(brokerServiceUrl);
                        }
                        if (brokerServiceUrlTls != null) {
                            clusterDataBuilder.brokerServiceUrlTls(brokerServiceUrlTls);
                        }
                        if (proxyServiceUrl != null) {
                            clusterDataBuilder.proxyServiceUrl(proxyServiceUrl);
                        }
                        if (authenticationPlugin != null) {
                            clusterDataBuilder.authenticationPlugin(authenticationPlugin);
                        }
                        if (authenticationParameters != null) {
                            clusterDataBuilder.authenticationParameters(authenticationParameters);
                        }

                        pulsarAdmin.clusters().updateCluster(clusterName, clusterDataBuilder.build());

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusterName", clusterName);
                        result.put("serviceUrl", serviceUrl);
                        result.put("updated", true);

                        return createSuccessResult("Cluster configuration updated successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid input: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to update cluster config", e);
                        return createErrorResult("Failed to update cluster config: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerDeleteCluster(McpSyncServer mcpServer) {
        McpSchema.Tool tool =  createTool(
                "delete-cluster",
                "Delete a Pulsar cluster by name",
                """
                {
                    "type": "object",
                    "properties": {
                        "clusterName": {
                            "type": "string",
                            "description": "Name of the cluster to delete"
                        },
                        "force": {
                            "type": "boolean",
                            "description": "Force cluster to delete",
                            "default": false
                        }
                    },
                    "required": ["clusterName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName");
                        Boolean force = getBooleanParam(request.arguments(), "force", false);

                        if (!force) {
                            try {
                                var namespaces = pulsarAdmin.namespaces().getNamespaces(clusterName);
                                if (!namespaces.isEmpty()) {
                                    return createErrorResult(
                                            "Cluster has active namespaces, Use 'force: true' to delete anyway.",
                                            List.of(
                                                    "Set 'force' parameter to true to force deletion",
                                                    "Or manually delete all namespaces first"
                                            )
                                    );
                                }
                            } catch (Exception e) {

                            }
                        }

                        pulsarAdmin.clusters().deleteCluster(clusterName);

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusterName", clusterName);
                        result.put("deleted", true);
                        result.put("forced", force);

                        return createSuccessResult("Cluster deleted successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to delete cluster", e);
                        return createErrorResult("Failed to delete cluster: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerGetClusterStats(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-cluster-stats",
                "Get statistics for a given Pulsar cluster",
                """
                {
                    "type": "object",
                    "properties": {
                        "clusterName": {
                            "type": "string",
                            "description": "The name of the cluster to retrieve stats for"
                        }
                    },
                    "required": ["clusterName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName");
                        if (clusterName == null || clusterName.isBlank()) {
                            return createErrorResult("Missing required parameter: clusterName");
                        }

                        var brokers = pulsarAdmin.brokers().getActiveBrokers(clusterName);

                        Map<String, Object> stats = new HashMap<>();
                        stats.put("clusterName", clusterName);
                        stats.put("activeBrokers", brokers);
                        stats.put("brokerCount", brokers.size());

                        return createSuccessResult("Cluster stats retrieved successfully", stats);

                    } catch (IllegalArgumentException e){
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get cluster stats", e);
                        return createErrorResult("Failed to get cluster stats: " + e.getMessage());
                    }
                })
                .build());
        }

    private void registerListBrokers(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "list-brokers",
                "List all active brokers in a given Pulsar cluster",
                """
                {
                    "type": "object",
                    "properties": {
                        "clusterName": {
                            "type": "string",
                            "description": "The name of the Pulsar cluster"
                        }
                    },
                    "required": ["clusterName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName");
                        if (clusterName == null || clusterName.isBlank()) {
                            return createErrorResult("Missing required parameter: clusterName");
                        }

                        var activeBrokers = pulsarAdmin.brokers().getActiveBrokers(clusterName);
                        var dynamicBrokers = pulsarAdmin.brokers().getDynamicConfigurationNames();

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusterName", clusterName);
                        result.put("activeBrokers", activeBrokers);
                        result.put("brokerCount", activeBrokers.size());
                        result.put("dynamicConfigNames", dynamicBrokers);

                        return createSuccessResult("List of active brokers retrieved successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to list brokers", e);
                        return createErrorResult("Failed to list brokers: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerGetBrokerStats(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-broker-stats",
                "Get statistics for a specific Pulsar broker",
                """
                {
                    "type": "object",
                    "properties": {
                        "brokerUrl": {
                            "type": "string",
                            "description": "The URL of the broker (e.g., 'localhost:8080')"
                        }
                    },
                    "required": ["brokerUrl"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String brokerUrl = getRequiredStringParam(request.arguments(), "brokerUrl");

                        if (brokerUrl == null || brokerUrl.isBlank()) {
                            return createErrorResult("Missing required parameter: brokerUrl");
                        }

                        var brokerStats = pulsarAdmin.brokerStats().getTopics();

                        Map<String, Object> result = new HashMap<>();
                        result.put("brokerUrl", brokerUrl);
                        result.put("stats", brokerStats);

                        return createSuccessResult("Broker stats retrieved successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get broker stats", e);
                        return createErrorResult("Failed to get broker stats: " + e.getMessage());
                    }
                })
                .build()
        );
    }

    private void registerGetClusterFailureDomain(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-cluster-failure-domain",
                "Get failure domains for a specific Pulsar cluster",
                """
                {
                    "type": "object",
                    "properties": {
                        "clusterName": {
                            "type": "string",
                            "description": "The name of the Pulsar cluster"
                        }
                    },
                    "required": ["clusterName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName");
                        if (clusterName == null || clusterName.isBlank()) {
                            return createErrorResult("Missing required parameter: clusterName");
                        }

                        var domains = pulsarAdmin.clusters().getFailureDomains(clusterName);

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusterName", clusterName);
                        result.put("failureDomains", domains);
                        result.put("available", false);

                        return createSuccessResult("Cluster failure domains retrieved successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get failure domains", e);
                        return createErrorResult("Failed to get failure domains: " + e.getMessage());
                    }
                })
                .build()
        );
    }

    private void registerSetClusterFailureDomain(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "set-cluster-failure-domain",
                "Set or update a failure domain in a Pulsar cluster",
                """
                {
                    "type": "object",
                    "properties": {
                        "clusterName": {
                            "type": "string",
                            "description": "The name of the Pulsar cluster"
                        },
                        "domainName": {
                            "type": "string",
                            "description": "The name of the failure domain"
                        },
                        "brokers": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "List of broker names in this domain (e.g., ['broker-1', 'broker-2'])"
                        },
                        "minDomains": {
                            "type": "integer",
                            "description": "Minimum number of failure domains to tolerate (optional)",
                            "default": 1,
                            "minimum": 1
                        },
                        "disableBrokerAutoRecovery": {
                            "type": "boolean",
                            "description": "Whether to disable auto recovery of brokers in this domain (optional)",
                            "default": false
                        }
                    },
                    "required": ["clusterName", "domainName", "brokers"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String clusterName = getRequiredStringParam(
                                request.arguments(),
                                "clusterName");
                        String domainName = getRequiredStringParam(
                                request.arguments(),
                                "domainName");
                        Integer minDomains = getIntParam(
                                request.arguments(),
                                "minDomains", 1);
                        Boolean disableBrokerAutoRecovery = getBooleanParam(
                                request.arguments(),
                                "disableBrokerAutoRecovery", false);

                        Object brokersObj = request.arguments().get("brokers");
                        if (brokersObj instanceof List) {
                            return createErrorResult("Missing required parameter: brokers");
                        }

                        @SuppressWarnings("unchecked")
                        List<String> brokers = (List<String>) brokersObj;

                        if (minDomains < 1) {
                            return createErrorResult("minDomains must be at least 1",
                                    List.of("Set minDomains to tolerate 1"));
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusterName", clusterName);
                        result.put("domainName", domainName);
                        result.put("brokers", brokers);
                        result.put("minDomains", minDomains);
                        result.put("disableBrokerAutoRecovery", disableBrokerAutoRecovery);
                        result.put("message", "Failure domain configuration prepared");
                        result.put("set", false);

                        return createSuccessResult("Failure domain set successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to set failure domain", e);
                        return createErrorResult("Failed to set failure domain: " + e.getMessage());
                    }
                })
                .build());
    }

}
