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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class SchemaTools extends BasePulsarTools {

  public SchemaTools(PulsarAdmin pulsarAdmin) {
    super(pulsarAdmin);
  }

  public void registerTools(McpSyncServer mcpServer) {
    registerGetSchemaInfo(mcpServer);
    registerGetSchemaVersion(mcpServer);
    registerAllSchemaVersions(mcpServer);
    registerDeleteSchema(mcpServer);
    registerTestSchemaCompatibility(mcpServer);
    registerUploadSchema(mcpServer);
  }

  private void registerGetSchemaInfo(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "get-schema-info",
            "Get schema info for a specific topic",
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
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        }
                    },
                    "required": ["tenant", "namespace", "topic"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String topic = buildFullTopicName(request.arguments());

                    SchemaInfo schemaInfo = pulsarAdmin.schemas().getSchemaInfo(topic);
                    Map<String, Object> result = new HashMap<>();
                    result.put("schemaType", schemaInfo.getType().name());
                    result.put(
                        "schema", Base64.getEncoder().encodeToString(schemaInfo.getSchema()));
                    result.put("properties", schemaInfo.getProperties());

                    return createSuccessResult("Schema info retrieved successfully", result);
                  } catch (IllegalArgumentException e) {
                    return createErrorResult("Invalid parameter: " + e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to get schema info", e);
                    return createErrorResult("Failed to get schema info: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerAllSchemaVersions(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "get-schema-AllVersions",
            "Get schema all versions",
            """
                {
                  "type": "object",
                  "properties": {
                    "topic": {
                      "type": "string",
                      "description": "Topic name (simple: 'orders', full: 'persistent://public/default/orders')"
                    }
                  },
                  "required": ["topic"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String topic = buildFullTopicName(request.arguments());

                    List<SchemaInfo> versions = pulsarAdmin.schemas().getAllSchemas(topic);
                    List<Map<String, Object>> versionList = new ArrayList<>();

                    for (int i = 0; i < versions.size(); i++) {
                      SchemaInfo schemaInfo = versions.get(i);
                      Map<String, Object> versionMap = new HashMap<>();
                      versionMap.put("versionIndex", i);
                      versionMap.put("type", schemaInfo.getType().name());
                      versionMap.put("schema", new String(schemaInfo.getSchema()));
                      versionMap.put("properties", schemaInfo.getProperties());
                      versionList.add(versionMap);
                    }

                    Map<String, Object> result = new HashMap<>();
                    result.put("topic", topic);
                    result.put("schemaVersions", versionList);

                    addTopicBreakdown(result, topic);
                    return createSuccessResult("Schema versions retrieved", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult("Schema not found for topic");
                  } catch (PulsarAdminException e) {
                    LOGGER.error("Failed to get schema version for topic", e);
                    return createErrorResult("Failed to get schema version: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerGetSchemaVersion(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "get-schema-version",
            "Get a specific schema version of a topic",
            """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
                        },
                        "versionIndex": {
                            "type": "integer",
                            "description": "Index of the schema version to retrieve (0-based)"
                        }
                    },
                    "required": ["topic", "versionIndex"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String topic = buildFullTopicName(request.arguments());
                    int versionIndex = getIntParam(request.arguments(), "versionIndex", 0);

                    List<SchemaInfo> schemaInfos = pulsarAdmin.schemas().getAllSchemas(topic);
                    if (versionIndex < 0 || versionIndex >= schemaInfos.size()) {
                      return createErrorResult(
                          "Invalid versionIndex: "
                              + versionIndex
                              + ", available versions: "
                              + schemaInfos.size());
                    }

                    SchemaInfo schemaInfo = schemaInfos.get(versionIndex);

                    Map<String, Object> result = new HashMap<>();
                    result.put("topic", topic);
                    result.put("versionIndex", versionIndex);
                    result.put("type", schemaInfo.getType().toString());
                    result.put("schema", new String(schemaInfo.getSchema()));
                    result.put("properties", schemaInfo.getProperties());
                    result.put("name", schemaInfo.getName());

                    addTopicBreakdown(result, topic);

                    return createSuccessResult(
                        "Fetched schema version " + versionIndex + " successfully", result);
                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (PulsarAdminException e) {
                    LOGGER.error("Failed to fetch schema version for topic", e);
                    return createErrorResult("Failed to fetch schema version: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerUploadSchema(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "upload-schema",
            "Upload a new schema to a topic",
            """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
                        },
                        "schema": {
                            "type": "string",
                            "description": "Schema content (usually JSON or AVRO schema string)"
                        },
                        "schemaType": {
                            "type": "string",
                            "description": "Schema type (e.g., AVRO, JSON, STRING)",
                            "enum": ["AVRO", "JSON", "STRING", "PROTOBUF", "KEY_VALUE", "BYTES"]
                        },
                        "properties": {
                            "type": "object",
                            "description": "Optional schema properties (key-value map)"
                        }
                    },
                    "required": ["topic", "schema", "schemaType"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String topic = buildFullTopicName(request.arguments());
                    String schemaStr = getRequiredStringParam(request.arguments(), "schema");
                    String schemaTypeStr =
                        getRequiredStringParam(request.arguments(), "schemaType");

                    SchemaType schemaType;
                    try {
                      schemaType = SchemaType.valueOf(schemaTypeStr.toUpperCase());
                    } catch (IllegalArgumentException e) {
                      return createErrorResult(
                          "Invalid schema type: " + schemaTypeStr,
                          List.of("Valid types: AVRO, JSON, STRING, PROTOBUF, BYTES"));
                    }

                    Map<String, String> props = null;
                    Object pObj = request.arguments().get("properties");
                    if (pObj instanceof Map<?, ?> m) {
                      props = new HashMap<>();
                      for (Map.Entry<?, ?> en : m.entrySet()) {
                        if (en.getKey() != null && en.getValue() != null) {
                          props.put(String.valueOf(en.getKey()), String.valueOf(en.getValue()));
                        }
                      }
                    }

                    SchemaInfo schemaInfo =
                        SchemaInfo.builder()
                            .name(topic)
                            .type(schemaType)
                            .schema(schemaStr.getBytes(StandardCharsets.UTF_8))
                            .properties(props)
                            .build();

                    pulsarAdmin.schemas().createSchema(topic, schemaInfo);

                    Map<String, Object> result = new HashMap<>();
                    result.put("topic", topic);
                    result.put("schema", schemaStr);
                    result.put("schemaType", schemaTypeStr);
                    result.put("uploaded", true);

                    addTopicBreakdown(result, topic);

                    return createSuccessResult(
                        "Schema uploaded successfully to topic: " + topic, null);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult("Invalid schemaType: " + e.getMessage());
                  } catch (PulsarAdminException e) {
                    LOGGER.error("Failed to upload schema to topic", e);
                    return createErrorResult("PulsarAdminException: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerDeleteSchema(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "delete-schema",
            "Delete the schema of a topic",
            """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
                        },
                        "force": {
                            "type": "boolean",
                            "description": "Force delete schema",
                            "default": false
                        }
                    },
                    "required": ["topic"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String topic = buildFullTopicName(request.arguments());
                    Boolean force = getBooleanParam(request.arguments(), "force", false);

                    pulsarAdmin.schemas().deleteSchema(topic, force);

                    Map<String, Object> result = new HashMap<>();
                    result.put("topic", topic);
                    result.put("deleted", true);
                    result.put("force", force);

                    addTopicBreakdown(result, topic);

                    return createSuccessResult(
                        "Schema deleted successfully from topic: " + topic, result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (PulsarAdminException e) {
                    LOGGER.error("Failed to delete schema from topic", e);
                    return createErrorResult("PulsarAdminException: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerTestSchemaCompatibility(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "test-schema-compatibility",
            "Test if a schema is compatible with the existing schema of a topic",
            """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
                        },
                        "schema": {
                            "type": "string",
                            "description": "Schema content to test (usually JSON or AVRO schema string)"
                        },
                        "schemaType": {
                            "type": "string",
                            "description": "Schema type (e.g., AVRO, JSON, STRING)",
                            "enum": ["AVRO", "JSON", "STRING", "PROTOBUF", "KEY_VALUE", "BYTES"]
                        }
                    },
                    "required": ["topic", "schema", "schemaType"]
                }
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String topic = buildFullTopicName(request.arguments());
                    String schemaStr = getRequiredStringParam(request.arguments(), "schema");
                    String schemaTypeStr =
                        getRequiredStringParam(request.arguments(), "schemaType");

                    SchemaType schemaType;
                    try {
                      schemaType = SchemaType.valueOf(schemaTypeStr.toUpperCase());
                    } catch (IllegalArgumentException e) {
                      return createErrorResult(
                          "Invalid schema type: " + schemaTypeStr,
                          List.of("Valid types: AVRO, JSON, STRING, PROTOBUF, BYTES"));
                    }

                    SchemaInfo schemaInfo =
                        SchemaInfo.builder()
                            .name(topic)
                            .type(schemaType)
                            .schema(schemaStr.getBytes(StandardCharsets.UTF_8))
                            .build();

                    boolean isCompatible =
                        pulsarAdmin
                            .schemas()
                            .testCompatibility(topic, schemaInfo)
                            .isCompatibility();

                    Map<String, Object> result = new HashMap<>();
                    result.put("topic", topic);
                    result.put("isCompatible", isCompatible);
                    result.put("schemaType", schemaType);

                    addTopicBreakdown(result, topic);

                    return createSuccessResult(
                        "Compatibility test result: " + isCompatible, result);
                  } catch (IllegalArgumentException e) {
                    return createErrorResult("Invalid parameter: " + e.getMessage());
                  } catch (PulsarAdminException e) {
                    LOGGER.error("Failed to test schema compatibility", e);
                    return createErrorResult("PulsarAdminException: " + e.getMessage());
                  }
                })
            .build());
  }
}
