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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.TenantInfo;

public class TenantTools extends BasePulsarTools {

    public TenantTools(PulsarAdmin pulsarAdmin) {
        super(pulsarAdmin);
    }

    public void registerTools(McpSyncServer mcpServer) {
        registerListTenant(mcpServer);
        registerGetTenantInfo(mcpServer);
        registerCreateTenant(mcpServer);
        registerUpdateTenant(mcpServer);
        registerDeleteTenant(mcpServer);
        registerGetTenantStats(mcpServer);
    }

    private void registerListTenant(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "list-tenants",
                "List all Pulsar tenants",
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
                        List<String> tenants = pulsarAdmin.tenants().getTenants();
                        Map<String, Object> result = Map.of(
                                "tenants", tenants,
                                "count", tenants.size()
                        );
                        return createSuccessResult("Tenant list retrieved successfully", result);
                    } catch (Exception e) {
                        LOGGER.error("Failed to list tenants", e);
                        return createErrorResult("Failed to list tenants", List.of(safeErrorMessage(e)));
                    }
                }).build()
        );
    }

    private void registerGetTenantInfo(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-tenant-info",
                "Get information about a Pulsar tenant",
                """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "The name of the tenant"
                        }
                    },
                    "required": ["tenant"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String tenant = getRequiredStringParam(request.arguments(), "tenant");
                        TenantInfo tenantInfo = pulsarAdmin.tenants().getTenantInfo(tenant);
                        Map<String, Object> result = Map.of(
                                "tenant", tenant,
                                "allowedClusters", tenantInfo.getAllowedClusters(),
                                "adminRoles", tenantInfo.getAdminRoles()
                        );
                        return createSuccessResult("Tenant info retrieved successfully", result);
                    } catch (PulsarAdminException.NotFoundException e) {
                        return createErrorResult("Tenant not found", List.of("Tenant does not exist"));
                    } catch (Exception e) {
                        LOGGER.error("Failed to get tenant info", e);
                        return createErrorResult("Failed to get tenant info", List.of(safeErrorMessage(e)));
                    }
                }).build()
        );
    }

    private void registerCreateTenant(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "create-tenant",
                "Create a new Pulsar tenant",
                """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "The name of the tenant to create"
                        },
                        "allowedClusters": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "List of clusters allowed for this tenant (optional)"
                        },
                        "adminRoles": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "List of admin roles for this tenant (optional)"
                        }
                    },
                    "required": ["tenant"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String tenant = getRequiredStringParam(request.arguments(), "tenant");

                        try {
                            pulsarAdmin.tenants().getTenantInfo(tenant);
                            return createErrorResult("Tenant already exists: " + tenant,
                                    List.of("Choose a different tenant name"));
                        } catch (PulsarAdminException.NotFoundException ignore) {
                        }

                        Set<String> adminRoles = getSetParam(request.arguments(), "adminRoles");
                        Set<String> allowedClusters = getSetParam(request.arguments(), "allowedClusters");

                        if (allowedClusters.isEmpty()) {
                            try {
                                List<String> availableClusters = pulsarAdmin.clusters().getClusters();
                                if (!availableClusters.isEmpty()) {
                                    allowedClusters = Set.copyOf(availableClusters);
                                } else {
                                    allowedClusters = Set.of("standalone");
                                }
                            } catch (Exception ex) {
                                LOGGER.warn("Failed to get clusters", ex);
                                allowedClusters = Set.of("standalone");
                            }
                        }

                        if (allowedClusters.isEmpty()) {
                            return createErrorResult("No available clusters for tenant creation",
                                    List.of("Specify allowedClusters or ensure clusters exist"));
                        }

                        TenantInfo tenantInfo = TenantInfo.builder()
                                .adminRoles(adminRoles)
                                .allowedClusters(allowedClusters)
                                .build();

                        pulsarAdmin.tenants().createTenant(tenant, tenantInfo);

                        return createSuccessResult("Tenant created successfully", Map.of(
                                "tenant", tenant,
                                "allowedClusters", allowedClusters,
                                "adminRoles", adminRoles,
                                "created", true
                        ));

                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid parameter: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to create tenant", e);
                        return createErrorResult("Failed to create tenant", List.of(safeErrorMessage(e)));
                    }
                }).build());
    }

    private void registerDeleteTenant(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "delete-tenant",
                "Delete a specific Pulsar tenant",
                """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "Name of the tenant to delete"
                        },
                        "force": {
                            "type": "boolean",
                            "description": "Whether to force delete the tenant even if it has namespaces",
                            "default": false
                        }
                    },
                    "required": ["tenant"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String tenant = getRequiredStringParam(request.arguments(), "tenant");
                        boolean force = getBooleanParam(request.arguments(), "force", false);

                        // 不能删除系统租户
                        if (isSystemTenant(tenant)) {
                            return createErrorResult(
                                    "System tenant cannot be deleted",
                                    List.of("Tenant: " + tenant)
                            );
                        }

                        // 检查命名空间
                        if (!force) {
                            List<String> namespaces = pulsarAdmin.namespaces().getNamespaces(tenant);
                            if (!namespaces.isEmpty()) {
                                return createErrorResult(
                                        "Tenant has namespaces. Use 'force=true' to delete.",
                                        List.of(
                                                "Namespaces: " + namespaces,
                                                "Set 'force' parameter to true to force deletion",
                                                "Or manually delete all namespaces first"
                                        )
                                );
                            }
                        }

                        // 执行删除
                        pulsarAdmin.tenants().deleteTenant(tenant);

                        Map<String, Object> resultData = new HashMap<>();
                        resultData.put("tenant", tenant);
                        resultData.put("force", force);
                        resultData.put("deleted", true);

                        return createSuccessResult(
                                "Tenant deleted successfully",
                                resultData
                        );

                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid parameter", List.of(e.getMessage()));
                    } catch (PulsarAdminException.ConflictException e) {
                        return createErrorResult(
                                "Tenant still has active namespaces",
                                List.of("Delete all namespaces first or use 'force=true'")
                        );
                    } catch (PulsarAdminException.NotFoundException e) {
                        return createErrorResult(
                                "Tenant not found",
                                List.of("Tenant does not exist or already deleted")
                        );
                    } catch (Exception e) {
                        LOGGER.error("Failed to delete tenant", e);
                        String errorMessage = (e.getMessage() != null && !e.getMessage().isBlank())
                                ? e.getMessage().split("\n")[0].trim()
                                : "Unknown error occurred";
                        return createErrorResult("Failed to delete tenant", List.of(errorMessage));
                    }
                }).build());
    }

    private void registerUpdateTenant(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "update-tenant",
                "Update the configuration of a specific Pulsar tenant",
                """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "Name of the tenant to update"
                        },
                        "adminRoles": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "List of new admin roles"
                        },
                        "allowedClusters": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "List of allowed clusters for the tenant"
                        }
                    },
                    "required": ["tenant"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String tenant = getRequiredStringParam(request.arguments(), "tenant");

                        TenantInfo currentInfo = pulsarAdmin.tenants().getTenantInfo(tenant);

                        Set<String> adminRoles = getSetParamOrDefault(request.arguments(),
                                "adminRoles", currentInfo.getAdminRoles());
                        Set<String> allowedClusters = getSetParamOrDefault(request.arguments(),
                                "allowedClusters", currentInfo.getAllowedClusters());

                        TenantInfo tenantInfo = TenantInfo.builder()
                                .adminRoles(adminRoles)
                                .allowedClusters(allowedClusters)
                                .build();

                        pulsarAdmin.tenants().updateTenant(tenant, tenantInfo);

                        return createSuccessResult("Tenant updated successfully", Map.of(
                                "tenant", tenant,
                                "adminRoles", adminRoles,
                                "allowedClusters", allowedClusters,
                                "updated", true
                        ));

                    } catch (PulsarAdminException.NotFoundException e) {
                        return createErrorResult("Tenant not found", List.of("Create the tenant first"));
                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid parameter: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to update tenant", e);
                        return createErrorResult("Failed to update tenant", List.of(safeErrorMessage(e)));
                    }
                }).build());
    }

    private void registerGetTenantStats(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-tenant-stats",
                "Get basic stats for a specific Pulsar tenant",
                """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "Name of the tenant to get statistics for"
                        }
                    },
                    "required": ["tenant"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String tenant = getRequiredStringParam(request.arguments(), "tenant");

                        List<String> namespaces = pulsarAdmin.namespaces().getNamespaces(tenant);

                        int totalTopics = 0;
                        Map<String, Integer> namespaceTopicCounts = new HashMap<>();

                        for (String namespace : namespaces) {
                            try {
                                List<String> topics = pulsarAdmin.topics().getList(namespace);
                                namespaceTopicCounts.put(namespace, topics.size());
                                totalTopics += topics.size();
                            } catch (Exception e) {
                                LOGGER.warn("Failed to get topics for namespace {}", namespace, e);
                                namespaceTopicCounts.put(namespace, 0);
                            }
                        }

                        return createSuccessResult("Tenant stats retrieved successfully", Map.of(
                                "tenant", tenant,
                                "namespaceCount", namespaces.size(),
                                "namespaces", namespaces,
                                "totalTopics", totalTopics,
                                "topicCounts", namespaceTopicCounts
                        ));

                    } catch (PulsarAdminException.NotFoundException e) {
                        return createErrorResult("Tenant not found", List.of("Ensure the tenant exists"));
                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid parameter: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get tenant stats", e);
                        return createErrorResult("Failed to get tenant stats", List.of(safeErrorMessage(e)));
                    }
                }).build());
    }

    private Set<String> getSetParam(Map<String, Object> args, String key) {
        Object obj = args.get(key);
        if (obj instanceof List<?> list) {
            return list.stream().filter(Objects::nonNull).map(String::valueOf).collect(Collectors.toSet());
        }
        return Set.of();
    }

    private Set<String> getSetParamOrDefault(Map<String, Object> args, String key, Set<String> defaultValue) {
        Set<String> value = getSetParam(args, key);
        return value.isEmpty() ? defaultValue : value;
    }

    private String safeErrorMessage(Exception e) {
        if (e.getMessage() == null
                || e.getMessage().isBlank()) {
            return "Unknown error occurred";
        }
        return e.getMessage().split("\n")[0].trim();
    }

    private boolean isSystemTenant(String tenant) {
        return tenant.equals("pulsar")
                || tenant.equals("public")
                || tenant.equals("sample");
    }

}
