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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.FailureDomain;

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

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
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
                        "clusterName": {
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
                        } catch (PulsarAdminException.NotFoundException ignore) {

                        } catch (PulsarAdminException e) {
                            return createErrorResult("Failed to verify cluster existence: " + e.getMessage());
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

                        ClusterData current;
                        try {
                            current = pulsarAdmin.clusters().getCluster(clusterName);
                        } catch (PulsarAdminException.NotFoundException e) {
                            return createErrorResult("Cluster not found: " + clusterName);
                        }

                        String finalServiceUrl = (serviceUrl != null && !serviceUrl.isBlank())
                                ? serviceUrl.trim()
                                : current.getServiceUrl();

                        var b = ClusterData.builder()
                                .serviceUrl(finalServiceUrl)
                                .serviceUrlTls((serviceUrlTls != null
                                        && !serviceUrlTls.isBlank())
                                        ? serviceUrlTls
                                        : current.getServiceUrlTls())
                                .brokerServiceUrl((brokerServiceUrl != null
                                        && !brokerServiceUrl.isBlank())
                                        ? brokerServiceUrl
                                        : current.getBrokerServiceUrl())
                                .brokerServiceUrlTls((brokerServiceUrlTls != null
                                        && !brokerServiceUrlTls.isBlank())
                                        ? brokerServiceUrlTls
                                        : current.getBrokerServiceUrlTls())
                                .proxyServiceUrl((proxyServiceUrl != null
                                        && !proxyServiceUrl.isBlank())
                                        ? proxyServiceUrl
                                        : current.getProxyServiceUrl());

                        if (authenticationPlugin != null && !authenticationPlugin.isBlank()) {
                            b.authenticationPlugin(authenticationPlugin);
                        } else if (current.getAuthenticationPlugin() != null) {
                            b.authenticationPlugin(current.getAuthenticationPlugin());
                        }
                        if (authenticationParameters != null && !authenticationParameters.isBlank()) {
                            b.authenticationParameters(authenticationParameters);
                        } else if (current.getAuthenticationParameters() != null) {
                            b.authenticationParameters(current.getAuthenticationParameters());
                        }

                        pulsarAdmin.clusters().updateCluster(clusterName, b.build());
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
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName").trim();
                        boolean force = getBooleanParam(request.arguments(), "force", false);

                        if (!force) {
                            List<String> tenants = pulsarAdmin.tenants().getTenants();
                            List<String> referencingTenants = new ArrayList<>();
                            for (String tenant : tenants) {
                                var info = pulsarAdmin.tenants().getTenantInfo(tenant);
                                var allowed = info != null ? info.getAllowedClusters() : null;
                                if (allowed != null && allowed.contains(clusterName)) {
                                    referencingTenants.add(tenant);
                                }
                            }

                            List<String> referencingNamespaces = new ArrayList<>();
                            for (String tenant : tenants) {
                                var nss = pulsarAdmin.namespaces().getNamespaces(tenant); // 正确：参数是 tenant
                                for (String ns : nss) {
                                    var repl = pulsarAdmin.namespaces().getNamespaceReplicationClusters(ns);
                                    if (repl != null && repl.contains(clusterName)) {
                                        referencingNamespaces.add(ns);
                                    }
                                }
                            }

                            if (!referencingTenants.isEmpty() || !referencingNamespaces.isEmpty()) {
                                StringBuilder sb = new StringBuilder();
                                sb.append("Cluster '").append(clusterName)
                                        .append("' is still referenced. Use 'force: true' to delete anyway.");
                                if (!referencingTenants.isEmpty()) {
                                    sb.append(" Referenced by tenants: ").append(referencingTenants);
                                }
                                if (!referencingNamespaces.isEmpty()) {
                                    sb.append(" Referenced by namespaces: ").append(referencingNamespaces);
                                }
                                return createErrorResult(sb.toString());
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
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName").trim();
                        if (clusterName.isEmpty()) {
                            return createErrorResult("clusterName cannot be blank");
                        }

                        try {
                            pulsarAdmin.clusters().getCluster(clusterName);
                        } catch (PulsarAdminException.NotFoundException e) {
                            return createErrorResult("Cluster '" + clusterName + "' not found");
                        }

                        List<String> active = new ArrayList<>(pulsarAdmin.brokers().getActiveBrokers(clusterName));
                        active.sort(String::compareTo);

                        String leader = null;
                        try {
                            leader = String.valueOf(pulsarAdmin.brokers().getLeaderBroker());
                        } catch (Exception ignore) {}

                        var dynamicConfigNames = pulsarAdmin.brokers().getDynamicConfigurationNames();
                        List<String> dynamicNamesSorted = dynamicConfigNames == null
                                ? List.of()
                                : dynamicConfigNames.stream().sorted().toList();

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusterName", clusterName);
                        result.put("activeBrokers", active);
                        result.put("brokerCount", active.size());
                        result.put("leaderBroker", leader);
                        result.put("dynamicConfigNames", dynamicNamesSorted);
                        result.put("available", !active.isEmpty());
                        result.put("timestamp", System.currentTimeMillis());

                        String msg = "List of active brokers retrieved successfully"
                                + (leader != null ? "" : " (leader not available)");
                        return createSuccessResult(msg, result);

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
                "Get failure domain(s) for a specific Pulsar cluster",
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
                      "description": "Optional. If set, only this failure domain will be returned"
                    },
                    "includeEmpty": {
                      "type": "boolean",
                      "description": "Include domains with empty broker list",
                      "default": true
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
                        String clusterName = getRequiredStringParam(request.arguments(), "clusterName").trim();
                        String domainName  = getStringParam(request.arguments(), "domainName");
                        boolean includeEmpty = getBooleanParam(request.arguments(), "includeEmpty", true);

                        if (clusterName.isEmpty()) {
                            return createErrorResult("clusterName cannot be blank");
                        }
                        if (domainName != null) {
                            domainName = domainName.trim();
                        }

                        try {
                            pulsarAdmin.clusters().getCluster(clusterName);
                        } catch (PulsarAdminException.NotFoundException e) {
                            return createErrorResult("Cluster '" + clusterName + "' not found");
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("clusterName", clusterName);

                        if (domainName != null && !domainName.isEmpty()) {
                            try {
                                FailureDomain fd =
                                        pulsarAdmin.clusters().getFailureDomain(clusterName, domainName);

                                Set<String> brokers = (fd != null && fd.getBrokers() != null)
                                        ? new HashSet<>(fd.getBrokers())
                                        : new HashSet<>();

                                if (!includeEmpty && brokers.isEmpty()) {
                                    result.put("domains", List.of());
                                    result.put("domainCount", 0);
                                    result.put("available", false);
                                    return createSuccessResult(
                                            "Domain exists but filtered by includeEmpty=false", result);
                                }

                                Map<String, Object> item = new HashMap<>();
                                item.put("domainName", domainName);
                                item.put("brokers", brokers.stream().sorted().toList());
                                item.put("brokerCount", brokers.size());

                                result.put("domains", List.of(item));
                                result.put("domainCount", 1);
                                result.put("available", true);

                                return createSuccessResult(
                                        "Cluster failure domain retrieved successfully", result);
                            } catch (PulsarAdminException.NotFoundException e) {
                                return createErrorResult(
                                        "Domain '"
                                        + domainName
                                        + "' not found in cluster '"
                                        + clusterName + "'");
                            }
                        } else {
                            Map<String, FailureDomain> raw =
                                    pulsarAdmin.clusters().getFailureDomains(clusterName);
                            if (raw == null) {
                                raw = Map.of();
                            }

                            List<Map<String, Object>> domains = new ArrayList<>();
                            int brokerTotal = 0;
                            List<String> emptyDomains = new ArrayList<>();

                            for (Map.Entry<String, FailureDomain> e : raw.entrySet()) {
                                String dn = e.getKey();
                                Set<String> brokers = (e.getValue() != null && e.getValue().getBrokers() != null)
                                        ? new HashSet<>(e.getValue().getBrokers())
                                        : new HashSet<>();

                                if (!includeEmpty && brokers.isEmpty()) {
                                    emptyDomains.add(dn);
                                    continue;
                                }
                                brokerTotal += brokers.size();

                                Map<String, Object> item = new HashMap<>();
                                item.put("domainName", dn);
                                item.put("brokers", brokers.stream().sorted().toList());
                                item.put("brokerCount", brokers.size());
                                domains.add(item);
                            }

                            domains.sort(Comparator.comparing(m -> (String) m.get("domainName")));

                            result.put("domains", domains);
                            result.put("domainCount", domains.size());
                            result.put("brokerTotal", brokerTotal);
                            if (!emptyDomains.isEmpty()) {
                                result.put("filteredEmptyDomains", emptyDomains.stream().sorted().toList());
                            }
                            result.put("available", !domains.isEmpty());

                            String msg = "Cluster failure domains retrieved successfully"
                                    + (includeEmpty ? "" : " (empty domains filtered)");
                            return createSuccessResult(msg, result);
                        }

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (PulsarAdminException e) {
                        return createErrorResult("Pulsar admin error: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get failure domains", e);
                        return createErrorResult("Failed to get failure domains: " + e.getMessage());
                    }
                })
                .build());
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
                            "items": {
                                "type": "string"
                            },
                            "description": "List of broker names in this domain (e.g., ['broker-1', 'broker-2'])",
                            "minItems": 1
                        },
                        "minDomains": {
                            "type": "integer",
                            "description": "Minimum number of failure domains to tolerate (optional)",
                            "default": 1,
                            "minimum": 1
                        },
                        "validateBrokers": {
                            "type": "boolean",
                            "description": "Validate brokers exist & not used in other domains (optional)",
                            "default": true
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
                        String clusterName  = getRequiredStringParam(request.arguments(), "clusterName").trim();
                        String domainName   = getRequiredStringParam(request.arguments(), "domainName").trim();
                        Integer minDomains  = getIntParam(request.arguments(), "minDomains", 1);
                        boolean validate    = getBooleanParam(request.arguments(),
                                "validateBrokers", true);

                        Object brokersObj = request.arguments().get("brokers");
                        if (!(brokersObj instanceof List<?> rawList)) {
                            return createErrorResult("Parameter 'brokers' must be a non-empty string list");
                        }
                        List<String> brokers = new ArrayList<>(rawList.size());
                        for (int i = 0; i < rawList.size(); i++) {
                            Object v = rawList.get(i);
                            if (!(v instanceof String s) || (s = s.trim()).isEmpty()) {
                                return createErrorResult("All brokers must be non-empty strings" + i);
                            }
                            brokers.add(s);
                        }
                        if (brokers.isEmpty()) {
                            return createErrorResult("brokers list cannot be empty");
                        }
                        if (minDomains < 1) {
                            return createErrorResult("minDomains must be at least 1");
                        }

                        try {
                            pulsarAdmin.clusters().getCluster(clusterName);
                            Set<String> brokerSet = new HashSet<>(brokers);
                            Map<String, FailureDomain> existing =
                                    pulsarAdmin.clusters().getFailureDomains(clusterName);

                            if (validate) {
                                for (Map.Entry<String, FailureDomain> e : existing.entrySet()) {
                                    String dn = e.getKey();
                                    if (dn.equals(domainName)) {
                                        continue;
                                    }
                                    Set<String> used = e.getValue().getBrokers();
                                    for (String b : brokerSet) {
                                        if (used != null && used.contains(b)) {
                                            return createErrorResult("broker '"
                                                    + b + "' already belongs to domain '"
                                                    + dn + "'");
                                        }
                                    }
                                }
                            }

                            boolean isUpdate = existing.containsKey(domainName);

                            FailureDomain domainObj;
                            domainObj = FailureDomain
                                    .builder().brokers(brokerSet).build();

                            if (isUpdate) {
                                pulsarAdmin.clusters().updateFailureDomain(clusterName, domainName, domainObj);
                            } else {
                                pulsarAdmin.clusters().createFailureDomain(clusterName, domainName, domainObj);
                            }

                            FailureDomain resultDomain =
                                    pulsarAdmin.clusters().getFailureDomain(clusterName, domainName);

                            boolean minMet = false;
                            try {
                                Map<String, FailureDomain> after =
                                        pulsarAdmin.clusters().getFailureDomains(clusterName);
                                minMet = after != null && after.size() >= minDomains;
                            } catch (Exception ignore) {}

                            Map<String, Object> result = new HashMap<>();
                            result.put("clusterName", clusterName);
                            result.put("domainName", domainName);
                            result.put("brokers", new ArrayList<>(brokers));
                            result.put("actualBrokers", new ArrayList<>(resultDomain.getBrokers())); // 真正生效
                            result.put("operation", isUpdate ? "update" : "create");
                            result.put("set", true);
                            result.put("timestamp", System.currentTimeMillis());
                            result.put("minDomains", minDomains);
                            result.put("minDomainsMet", minMet);

                            String msg = "Failure domain " + (isUpdate ? "updated" : "created") + " successfully"
                                    + (minMet ? "" : " (warning: minDomains not met)");
                            return createSuccessResult(msg, result);

                        } catch (PulsarAdminException e) {
                            return createErrorResult("Pulsar admin error: " + e.getMessage());
                        }

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to process set failure domain request", e);
                        return createErrorResult("Failed to process request: " + e.getMessage());
                    }
                })
                .build());
    }

}
