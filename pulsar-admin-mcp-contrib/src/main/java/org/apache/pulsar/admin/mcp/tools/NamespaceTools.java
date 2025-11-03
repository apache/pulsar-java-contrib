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
package org.apache.pulsar.admin.mcp.tools;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.RetentionPolicies;

public class NamespaceTools extends BasePulsarTools {

  public NamespaceTools(PulsarAdmin pulsarAdmin) {
    super(pulsarAdmin);
  }

  public void registerTools(McpSyncServer mcpServer) {
    registerListNamespaces(mcpServer);
    registerGetNamespaceInfo(mcpServer);
    registerCreateNamespace(mcpServer);
    registerDeleteNamespace(mcpServer);
    registerSetRetentionPolicy(mcpServer);
    registerGetRetentionPolicy(mcpServer);
    registerSetBacklogQuota(mcpServer);
    registerGetBacklogQuota(mcpServer);
    registerClearNamespaceBacklog(mcpServer);
    registerNamespaceStats(mcpServer);
  }

  private void registerListNamespaces(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "list-namespaces",
            "List all namespaces under a given tenant",
            """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "The name of the tenant"
                        }
                    },
                    "required": []
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String tenant = getStringParam(request.arguments(), "tenant");
                    if (tenant != null) {
                      tenant = tenant.trim();
                    }
                    List<String> namespaces;
                    if (tenant != null && !tenant.trim().isEmpty()) {
                      namespaces = pulsarAdmin.namespaces().getNamespaces(tenant);
                      if (namespaces == null) {
                        namespaces = List.of();
                      }
                    } else {
                      List<String> tenants = pulsarAdmin.tenants().getTenants();
                      if (tenants == null) {
                        tenants = List.of();
                      }
                      namespaces = new ArrayList<>();
                      for (String namespace : tenants) {
                        try {
                          namespaces.addAll(pulsarAdmin.namespaces().getNamespaces(namespace));
                        } catch (Exception e) {
                          LOGGER.warn("Failed to get namespaces for tenant " + namespace, e);
                        }
                      }
                    }

                    Map<String, Object> result = new HashMap<>();
                    result.put("tenant", tenant);
                    result.put("namespaces", namespaces);
                    result.put("count", namespaces.size());

                    return createSuccessResult("Namespaces retrieved successfully", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult("Invalid parameter: " + e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to list namespaces", e);
                    return createErrorResult("Failed to list namespaces: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerGetNamespaceInfo(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "get-namespace-info",
            "Get detailed info of a namespace under a tenant",
            """
                {
                    "type": "object",
                    "properties": {
                         "tenant": {
                              "type": "string",
                              "description": "Tenant name (default: 'public')",
                              "default": "public"
                          },
                          "namespace": {
                              "type": "string",
                              "description": "Namespace name or full path ('orders' or 'public/orders')",
                              "default": "default"
                          }
                    },
                    "required": ["tenant", "namespace"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String fullNamespace = resolveNamespace(request.arguments());

                    Map<String, Object> details = new HashMap<>();
                    details.put("policies", pulsarAdmin.namespaces().getPolicies(fullNamespace));
                    details.put(
                        "backlogQuotaMap",
                        pulsarAdmin.namespaces().getBacklogQuotaMap(fullNamespace));
                    details.put("retention", pulsarAdmin.namespaces().getRetention(fullNamespace));
                    details.put(
                        "persistence", pulsarAdmin.namespaces().getPersistence(fullNamespace));
                    details.put(
                        "maxConsumersPerSubscription",
                        pulsarAdmin.namespaces().getMaxConsumersPerSubscription(fullNamespace));
                    details.put(
                        "maxConsumersPerTopic",
                        pulsarAdmin.namespaces().getMaxConsumersPerTopic(fullNamespace));
                    details.put(
                        "maxProducersPerTopic",
                        pulsarAdmin.namespaces().getMaxProducersPerTopic(fullNamespace));

                    return createSuccessResult("Namespace info retrieved successfully", details);
                  } catch (IllegalArgumentException e) {
                    return createErrorResult("Invalid parameter: " + e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to get namespace info", e);
                    return createErrorResult("Failed to get namespace info: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerCreateNamespace(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "create-namespace",
            "Create a new namespace under a given tenant",
            """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "Tenant name (default: 'public')",
                            "default": "public"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name or full path ('orders' or 'public/orders')",
                            "default": "default"
                        }
                    },
                    "required": ["namespace"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String fullNamespace = resolveNamespace(request.arguments());

                    pulsarAdmin.namespaces().createNamespace(fullNamespace);

                    String[] parts = fullNamespace.split("/");
                    String tenant = parts[0];
                    String namespace = parts[1];

                    Map<String, Object> result = new HashMap<>();
                    result.put("tenant", tenant);
                    result.put("namespace", namespace);
                    result.put("created", true);

                    return createSuccessResult("Namespace created successfully", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to create namespace", e);
                    return createErrorResult("Failed to create namespace: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerDeleteNamespace(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "delete-namespace",
            "Delete a namespace under a given tenant",
            """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "Tenant name (default: 'public')",
                            "default": "public"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name or full path ('orders' or 'public/orders')",
                            "default": "default"
                        },
                        "force": {
                            "type": "boolean",
                            "description": "Whether to force deletion (default: false)",
                            "default": false
                        }
                    },
                    "required": ["namespace"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String fullNamespace = resolveNamespace(request.arguments());
                    boolean force = getBooleanParam(request.arguments(), "force", false);

                    String[] parts = fullNamespace.split("/");
                    String tenant = parts.length > 0 ? parts[0] : "";
                    String namespace = parts.length > 1 ? parts[1] : "";

                    if (isSystemNamespace(namespace)) {
                      return createErrorResult("Cannot delete system namespace: " + namespace);
                    }

                    pulsarAdmin.namespaces().deleteNamespace(fullNamespace, force);

                    Map<String, Object> result = new HashMap<>();
                    result.put("tenant", tenant);
                    result.put("namespace", namespace);
                    result.put("force", force);
                    result.put("deleted", true);

                    return createSuccessResult("Namespace deleted successfully", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to delete namespace", e);
                    return createErrorResult("Failed to delete namespace: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerSetRetentionPolicy(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "set-retention-policy",
            "Set the retention policy for a specific namespace",
            """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "Tenant name (default: 'public')",
                            "default": "public"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name or full path ('orders' or 'public/orders')",
                            "default": "default"
                        },
                        "retentionTimeInMinutes": {
                            "type": "integer",
                            "description": "How long to retain data in minutes",
                            "minimum": 0
                        },
                        "retentionSizeInMB": {
                            "type": "integer",
                            "description": "How much data to retain in MB",
                            "minimum": 0
                        }
                    },
                    "required": ["tenant", "namespace"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String fullNamespace = resolveNamespace(request.arguments());

                    String[] parts = fullNamespace.split("/", 2);
                    String tenant = parts.length > 0 ? parts[0] : "";
                    String namespace = parts.length > 1 ? parts[1] : "";

                    Integer retentionTime =
                        getIntParam(request.arguments(), "retentionTimeInMinutes", -1);
                    Integer retentionSize =
                        getIntParam(request.arguments(), "retentionSizeInMB", -1);
                    if (retentionTime == null) {
                      retentionTime = -1;
                    }
                    if (retentionSize == null) {
                      retentionSize = -1;
                    }

                    RetentionPolicies policies =
                        new RetentionPolicies(retentionTime, retentionSize);
                    pulsarAdmin.namespaces().setRetention(fullNamespace, policies);

                    Map<String, Object> result = new HashMap<>();
                    result.put("tenant", tenant);
                    result.put("namespace", namespace);
                    result.put("retentionTime", retentionTime);
                    result.put("retentionSize", retentionSize);

                    return createSuccessResult("Retention policy set successfully", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to set retention policy", e);
                    return createErrorResult("Failed to set retention policy: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerGetRetentionPolicy(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "get-retention-policy",
            "Get the retention policy for a specific namespace",
            """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "The tenant name"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name or full path ('orders' or 'public/orders')",
                            "default": "default"
                        }
                    },
                    "required": ["tenant", "namespace"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String fullNamespace = resolveNamespace(request.arguments());

                    String[] parts = fullNamespace.split("/", 2);
                    String tenant = parts.length > 0 ? parts[0] : "";
                    String namespace = parts.length > 1 ? parts[1] : "";

                    RetentionPolicies policies =
                        pulsarAdmin.namespaces().getRetention(fullNamespace);

                    Map<String, Object> result = new HashMap<>();
                    result.put("tenant", tenant);
                    result.put("namespace", namespace);

                    if (policies != null) {
                      result.put("retentionTimeInMinutes", policies.getRetentionTimeInMinutes());
                      result.put("retentionSizeInMB", policies.getRetentionSizeInMB());
                    } else {
                      result.put("message", "No retention policy configured for this namespace.");
                    }

                    return createSuccessResult("Retention policy fetched successfully", result);
                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to get retention policy", e);
                    return createErrorResult("Failed to get retention policy: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerSetBacklogQuota(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "set-backlog-quota",
            "Set backlog quota for a specific namespace",
            """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "The tenant name"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name or full path ('orders' or 'public/orders')",
                            "default": "default"
                        },
                        "limitSizeInBytes": {
                            "type": "integer",
                            "description": "Backlog quota limit in bytes",
                            "minimum": 0
                        },
                        "policy": {
                            "type": "string",
                            "description": "policy: producer_request_hold, producer_exception, or consumer_eviction"
                       }
                    },
                    "required": ["tenant", "namespace", "limitSizeInBytes", "policy"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String fullNamespace = resolveNamespace(request.arguments());

                    String[] parts = fullNamespace.split("/", 2);
                    String tenant = parts.length > 0 ? parts[0] : "";
                    String namespace = parts.length > 1 ? parts[1] : "";

                    Integer limitSize = getIntParam(request.arguments(), "limitSizeInBytes", 0);
                    String policyStr = getRequiredStringParam(request.arguments(), "policy");

                    if (limitSize == null || limitSize <= 0) {
                      return createErrorResult("Limit size must be greater than 0.");
                    }

                    BacklogQuota.RetentionPolicy policy;
                    switch (policyStr.toLowerCase()) {
                      case "producer_request_hold":
                        policy = BacklogQuota.RetentionPolicy.producer_request_hold;
                        break;
                      case "producer_exception":
                        policy = BacklogQuota.RetentionPolicy.producer_exception;
                        break;
                      case "consumer_backlog_eviction":
                        policy = BacklogQuota.RetentionPolicy.consumer_backlog_eviction;
                        break;
                      default:
                        return createErrorResult(
                            "Invalid policy:" + policyStr,
                            List.of(
                                "Valid policies: producer_request_hold, "
                                    + "producer_exception, consumer_backlog_eviction"));
                    }

                    BacklogQuota quota =
                        BacklogQuota.builder().limitSize(limitSize).retentionPolicy(policy).build();

                    pulsarAdmin.namespaces().setBacklogQuota(fullNamespace, quota);

                    Map<String, Object> result = new HashMap<>();
                    result.put("tenant", tenant);
                    result.put("namespace", namespace);
                    result.put("limitSizeInBytes", limitSize);
                    result.put("policy", policy.name());

                    return createSuccessResult("Backlog quota set successfully", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to set backlog quota", e);
                    return createErrorResult("Failed to set backlog quota: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerGetBacklogQuota(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "get-backlog-quota",
            "Get backlog quota for a specific namespace",
            """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "The tenant name"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name or full path ('orders' or 'public/orders')",
                            "default": "default"
                        }
                    },
                    "required": ["tenant", "namespace"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String fullNamespace = resolveNamespace(request.arguments());

                    String[] parts = fullNamespace.split("/", 2);
                    String tenant = parts.length > 0 ? parts[0] : "";
                    String namespace = parts.length > 1 ? parts[1] : "";

                    Map<BacklogQuota.BacklogQuotaType, BacklogQuota> quotas =
                        pulsarAdmin.namespaces().getBacklogQuotaMap(fullNamespace);

                    Map<String, Object> result = new HashMap<>();
                    result.put("tenant", tenant);
                    result.put("namespace", namespace);

                    if (quotas == null || quotas.isEmpty()) {
                      result.put("message", "No backlog quota configured");
                      result.put("quotas", Map.of());
                    } else {
                      Map<String, Object> quotaInfo = new HashMap<>();
                      quotas.forEach(
                          (type, quota) -> {
                            Map<String, Object> info = new HashMap<>();
                            info.put("limitSize", quota.getLimitSize());
                            info.put("policy", quota.getPolicy().toString());
                            quotaInfo.put(type.toString(), info);
                          });
                      result.put("quotas", quotaInfo);
                    }

                    return createSuccessResult("Backlog quota retrieved successfully", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to get backlog quota", e);
                    return createErrorResult("Failed to get backlog quota: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerClearNamespaceBacklog(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "clear-namespace-backlog",
            "Clear the backlog for a specific namespace",
            """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "The tenant name"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name or full path ('orders' or 'public/orders')",
                            "default": "default"
                        },
                        "subscriptionName": {
                             "type": "string",
                             "description": "clear backlog only for this subscription"
                        }
                    },
                    "required": ["tenant", "namespace"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String fullNamespace = resolveNamespace(request.arguments());

                    String[] parts = fullNamespace.split("/", 2);
                    String tenant = parts.length > 0 ? parts[0] : "";
                    String namespace = parts.length > 1 ? parts[1] : "";
                    String subscriptionName =
                        getStringParam(request.arguments(), "subscriptionName");

                    if (subscriptionName != null && !subscriptionName.trim().isEmpty()) {
                      pulsarAdmin
                          .namespaces()
                          .clearNamespaceBacklogForSubscription(fullNamespace, subscriptionName);
                    } else {
                      pulsarAdmin.namespaces().clearNamespaceBacklog(fullNamespace);
                    }

                    Map<String, Object> result = new HashMap<>();
                    result.put("tenant", tenant);
                    result.put("namespace", namespace);

                    return createSuccessResult("Namespace backlog cleared successfully", result);
                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to clear namespace backlog", e);
                    return createErrorResult(
                        "Failed to clear namespace backlog: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerNamespaceStats(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "get-namespace-stats",
            "Get statistics for a specific namespace",
            """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "The tenant name"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name or full path ('orders' or 'public/orders')",
                            "default": "default"
                        }
                    },
                    "required": ["tenant", "namespace"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String fullNamespace = resolveNamespace(request.arguments());

                    String[] parts = fullNamespace.split("/", 2);
                    String tenant = parts.length > 0 ? parts[0] : "";
                    String namespace = parts.length > 1 ? parts[1] : "";

                    List<String> topics = pulsarAdmin.topics().getList(fullNamespace);
                    if (topics == null) {
                      topics = List.of();
                    }

                    long persistentTopics =
                        topics.stream().filter(t -> t.startsWith("persistent://")).count();
                    long nonPersistentTopics =
                        topics.stream().filter(t -> t.startsWith("non-persistent://")).count();

                    Map<String, Object> result = new HashMap<>();
                    result.put("namespace", namespace);
                    result.put("tenant", tenant);
                    result.put("persistentTopics", persistentTopics);
                    result.put("nonPersistentTopics", nonPersistentTopics);
                    result.put("topics", topics);

                    return createSuccessResult("Namespace stats retrieved successfully", result);
                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to get namespace stats", e);
                    return createErrorResult("Failed to get namespace stats: " + e.getMessage());
                  }
                })
            .build());
  }

  private boolean isSystemNamespace(String namespace) {
    return namespace.equals("public/default")
        || namespace.equals("public/functions")
        || namespace.equals("public/system");
  }
}
