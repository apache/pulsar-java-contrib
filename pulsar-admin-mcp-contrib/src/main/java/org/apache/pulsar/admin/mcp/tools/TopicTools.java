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
import org.apache.pulsar.common.policies.data.TopicStats;

public class TopicTools extends BasePulsarTools {

    public TopicTools(PulsarAdmin pulsarAdmin) {
        super(pulsarAdmin);
    }

    public void registerTools(McpSyncServer mcpServer) {
        registerListTopics(mcpServer);
        registerCreateTopics(mcpServer);
        registerDeleteTopics(mcpServer);
        registerGetTopicStats(mcpServer);
        registerGetTopicMetadata(mcpServer);
        registerUpdateTopicPartitions(mcpServer);
        registerCompactTopic(mcpServer);
        registerUnloadTopic(mcpServer);
        registerGetTopicBacklog(mcpServer);
        registerExpireTopicMessages(mcpServer);
        registerPeekTopicMessages(mcpServer);
        registerResetTopicCursor(mcpServer);
        registerGetTopicInternalStats(mcpServer);
        registerGetPartitionedMetadata(mcpServer);
    }

    private void registerListTopics(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "list-topics",
                "List all topics under a specific namespace",
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
                            "required": []
                        }
                        """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String namespace = resolveNamespace(request.arguments());

                        List<String> topics = pulsarAdmin.topics().getList(namespace);
                        if (topics == null) {
                            topics = List.of();
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("namespace", namespace);
                        result.put("topics", topics);
                        result.put("count", topics.size());

                        return createSuccessResult("Topics listed successfully", result);
                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to list topics", e);
                        return createErrorResult("Failed to list topics: " + e.getMessage());
                    }
                }).build());
    }

    private void registerCreateTopics(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "create-topics",
                "Create one or more topics under a specific namespace",
                """
                      {
                            "type": "object",
                            "properties": {
                                "namespace": {
                                    "type": "string",
                                    "description": "Namespace name or full path ('orders' or 'public/orders')",
                                    "default": "default"
                                },
                                "topic": {
                                     "type": "string",
                                     "description": "Topic name('orders' or 'persistent://public/default/orders')"
                                },
                                "persistent": {
                                    "type": "boolean",
                                    "description": "Whether topic should be persistent (default: true)",
                                    "default": true
                                },
                                "partitions": {
                                    "type": "integer",
                                    "description": "Number of partitions for each topic (0 means non-partitioned)",
                                    "minimum": 0
                                }
                            },
                            "required": ["topic"]
                      }
                      """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());

                        boolean persistent = getBooleanParam(request.arguments(), "persistent", true);
                        if (topic.startsWith("persistent://") && !persistent) {
                            topic = "non-" + topic;
                        } else if (topic.startsWith("non-persistent://") && persistent) {
                            topic = topic.replaceFirst("non-persistent://", "persistent://");
                        }

                        Integer partitions = getIntParam(request.arguments(), "partitions", 0);

                        if (partitions > 0) {
                            pulsarAdmin.topics().createPartitionedTopic(topic, partitions);
                        } else {
                            pulsarAdmin.topics().createNonPartitionedTopic(topic);
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("created", true);
                        result.put("partitions", partitions);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Topics created successfully", result);
                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to create topics", e);
                        return createErrorResult("Failed to create topics: " + e.getMessage());
                    }
                }).build());
    }

    private void registerDeleteTopics(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "delete-topics",
                "Delete one or more topics",
                """
                {
                    "type": "object",
                    "properties": {
                        "tenant": {
                            "type": "string",
                            "description": "The tenant name",
                            "default": "public"
                        },
                        "namespace": {
                            "type": "string",
                            "description": "Namespace name or full path ('orders' or 'public/orders')",
                            "default": "default"
                        },
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "force": {
                            "type": "boolean",
                            "description": "Force delete topic even if it has active subscriptions (default: false)",
                            "default": false
                        },
                        "persistent": {
                            "type": "boolean",
                            "description": "Whether the topic is persistent (default: true)",
                            "default": true
                        }
                    },
                    "required": ["topic"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());
                        Boolean force = getBooleanParam(request.arguments(), "force", false);

                        var metadata = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                        if (metadata != null && metadata.partitions > 0) {
                            pulsarAdmin.topics().deletePartitionedTopic(topic, force);
                        } else {
                            pulsarAdmin.topics().delete(topic, force);
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("deleted", true);
                        result.put("force", force);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Topic deleted successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to delete topic", e);
                        return createErrorResult("Failed to delete topic: " + e.getMessage());
                    }
                }).build());
    }

    private void registerGetTopicStats(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-topic-stats",
                "Get statistics for a specific topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                        }
                    },
                    "required": ["topic"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());

                        var meta = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);

                        if (meta != null && meta.partitions > 0) {
                            var ps = pulsarAdmin.topics().getPartitionedStats(topic, false);
                            result.put("msgRateIn", ps.getMsgRateIn());
                            result.put("msgRateOut", ps.getMsgRateOut());
                            result.put("msgThroughputIn", ps.getMsgThroughputIn());
                            result.put("msgThroughputOut", ps.getMsgThroughputOut());
                            result.put("storageSize", ps.getStorageSize());
                        } else {
                            TopicStats stats = pulsarAdmin.topics().getStats(topic);
                            result.put("msgRateIn", stats.getMsgRateIn());
                            result.put("msgRateOut", stats.getMsgRateOut());
                            result.put("msgThroughputIn", stats.getMsgThroughputIn());
                            result.put("msgThroughputOut", stats.getMsgThroughputOut());
                            result.put("storageSize", stats.getStorageSize());
                            result.put("subscriptions", stats.getSubscriptions()); // 可能为 null，直接透传
                            result.put("publishers", stats.getPublishers());
                            result.put("replication", stats.getReplication());
                        }

                        return createSuccessResult("Topic stats retrieved successfully", result);
                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get topic stats", e);
                        return createErrorResult("Failed to get topic stats: " + e.getMessage());
                    }
                }).build());
    }

    private void registerGetTopicMetadata(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-topic-metadata",
                "Get metadata information for a specific topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                        }
                    },
                    "required": ["topic"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());

                        var metadata = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                        int partitions = (metadata == null) ? 0 : metadata.partitions;

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("partitions", partitions);
                        result.put("isPartitioned", partitions > 0);

                        return createSuccessResult("Topic metadata fetched successfully", result);
                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get topic metadata", e);
                        return createErrorResult("Failed to get topic metadata: " + e.getMessage());
                    }
                }).build());
    }

    private void registerUpdateTopicPartitions(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "update-topic-partitions",
                "Update the number of partitions for a partitioned Pulsar topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple: 'orders' or full: 'persistent://public/default/orders')"
                        },
                        "partitions": {
                            "type": "integer",
                            "description": "New number of partitions (must be greater than current partition count)",
                            "minimum": 1
                        },
                        "force": {
                            "type": "boolean",
                            "description": "Force update even if there are active consumers (default: false)",
                            "default": false
                        }
                    },
                    "required": ["topic", "partitions"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());
                        Integer partitions = getIntParam(request.arguments(), "partitions", 0);

                        if (partitions <= 0) {
                            return createErrorResult("Invalid partitions parameter: "
                                    + "must be at least 1");
                        }

                        var currentMetadata = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                        int currentPartitions = currentMetadata.partitions;

                        if (currentPartitions == 0) {
                            return createErrorResult("Topic is not partitioned. "
                                    + "Use create-partitioned-topic to create a partitioned topic.");
                        }

                        if (partitions <= currentPartitions) {
                            return createErrorResult("New partition count ("
                                    + partitions
                                    + ") must be greater than current partition count ("
                                    + currentPartitions
                                    + ")");
                        }

                        pulsarAdmin.topics().updatePartitionedTopic(topic, partitions);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("previousPartitions", currentPartitions);
                        result.put("newPartitions", partitions);
                        result.put("updated", true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Topic partitions updated successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid input parameter: " + e.getMessage());
                    }  catch (Exception e) {
                        LOGGER.error("Failed to update topic partitions", e);
                        return createErrorResult("Failed to update topic partitions: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerCompactTopic(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "compact-topic",
                "Compact a specified topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                        }
                    },
                    "required": ["topic"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());
                        var meta = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                        if (meta != null && meta.partitions > 0) {
                            for (int i = 0; i < meta.partitions; i++) {
                                pulsarAdmin.topics().triggerCompaction(topic + "-partition-" + i);
                            }
                        } else {
                            pulsarAdmin.topics().triggerCompaction(topic);
                        }
                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("compactionTriggered", true);
                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Compaction triggered successfully for topic: ", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to compact topic", e);
                        return createErrorResult("Failed to compact topic: " + e.getMessage());
                    }
                }).build());
    }

    private void registerUnloadTopic(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "unload-topic",
                "Unload a specified topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                        }
                    },
                    "required": ["topic"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());

                        var meta = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                        if (meta != null && meta.partitions > 0) {
                            for (int i = 0; i < meta.partitions; i++) {
                                pulsarAdmin.topics().unload(topic + "-partition-" + i);
                            }
                        } else {
                            pulsarAdmin.topics().unload(topic);
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("unloaded", true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Topic unloaded successfully: ", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to unload topic", e);
                        return createErrorResult("Failed to unload topic: " + e.getMessage());
                    }
                }).build());
    }

    private void registerGetTopicBacklog(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-topic-backlog",
                "Get the backlog size of a specified topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                           "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                       }
                    },
                    "required": ["topic"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());

                        TopicStats stats = pulsarAdmin.topics().getStats(topic);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);

                        Map<String, Object> subscriptionBacklogs = new HashMap<>();
                        long totalBacklog = 0;

                        for (var entry : stats.getSubscriptions().entrySet()) {
                            String subscriptionName = entry.getKey();
                            var subscriptionStats = entry.getValue();

                            long backlog = subscriptionStats.getBacklogSize();
                            totalBacklog += backlog;

                            Map<String, Object> subInfo = new HashMap<>();
                            subInfo.put("backlog", backlog);
                            subInfo.put("type", subscriptionStats.getType());
                            subInfo.put("consumers", subscriptionStats.getConsumers());

                            subscriptionBacklogs.put(subscriptionName, subInfo);
                        }

                        result.put("totalBacklog", totalBacklog);
                        result.put("subscriptionBacklogs", subscriptionBacklogs);
                        result.put("subscriptionCount", stats.getSubscriptions().size());

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Topic backlog fetched successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get topic backlog", e);
                        return createErrorResult("Failed to get topic backlog: " + e.getMessage());
                    }
                }).build());
    }

    private void registerExpireTopicMessages(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "expire-topic-messages",
                "Expire messages for all subscriptions on a topic older than a given time",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                         },
                        "expireTimeInSeconds": {
                            "type": "integer",
                            "description": "Messages older than this number of seconds will be marked as expired",
                            "default": 0
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Subscription name to expire message for"
                        }
                    },
                    "required": ["topic", "subscriptionName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());
                        String subscriptionName = getRequiredStringParam(request.arguments(), "subscriptionName");
                        Integer expireTimeInSeconds = getIntParam(request.arguments(), "expireTimeInSeconds", 0);

                        if (expireTimeInSeconds == null || expireTimeInSeconds <= 0) {
                            return createErrorResult("expireTimeInSeconds must be > 0");
                        }

                        var meta = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                        if (meta != null && meta.partitions > 0) {
                            for (int i = 0; i < meta.partitions; i++) {
                                pulsarAdmin.topics().expireMessages(topic
                                        + "-partition-"
                                        + i, subscriptionName, expireTimeInSeconds);
                            }
                        } else {
                            pulsarAdmin.topics().expireMessages(topic, subscriptionName, expireTimeInSeconds);
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("expireTimeInSeconds", expireTimeInSeconds);
                        result.put("expired", true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Expired messages on topic successfully", result);
                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to expire messages on topic", e);
                        return createErrorResult("Failed to expire messages on topic: " + e.getMessage());
                    }
                }).build());
    }

    private void registerPeekTopicMessages(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "peek-topic-messages",
                "Peek messages from a subscription of a topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                        },
                        "subscription": {
                            "type": "string",
                            "description": "The name of the subscription"
                        },
                        "count": {
                            "type": "integer",
                            "description": "Number of messages to peek",
                            "minimum": 1
                        }
                    },
                    "required": ["topic", "subscription", "count"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());
                        String subscription = getRequiredStringParam(request.arguments(), "subscription");
                        Integer count = getIntParam(request.arguments(), "count", 1);

                        if (count == null || count <= 0) {
                            return createErrorResult("count must be >= 1");
                        }

                        var raw = pulsarAdmin.topics().peekMessages(topic, subscription, count);
                        List<Map<String, Object>> messages = new ArrayList<>();
                        if (raw != null) {
                            for (var msg : raw) {
                                Map<String, Object> m = new HashMap<>();
                                try {
                                    m.put("messageId", String.valueOf(msg.getMessageId()));
                                    m.put("publishTime", msg.getPublishTime());
                                    m.put("eventTime", msg.getEventTime());
                                    m.put("key", msg.getKey());
                                    m.put("properties", msg.getProperties());
                                    byte[] payload = msg.getData();
                                    m.put("payloadBase64", payload == null
                                            ? null : java.util.Base64.getEncoder().encodeToString(payload));
                                } catch (Throwable t) {
                                    m.put("error", "Failed to materialize message: " + t.getMessage());
                                }
                                messages.add(m);
                            }
                        }

                        Map<String, Object> results = new HashMap<>();
                        results.put("topic", topic);
                        results.put("subscription", subscription);
                        results.put("count", count);
                        results.put("messages", messages);

                        addTopicBreakdown(results, topic);

                        return createSuccessResult("Messages peeked successfully", results);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Error peeking topic messages", e);
                        return createErrorResult("Failed to peek messages: " + e.getMessage());
                    }
                }).build());
    }

    private void registerResetTopicCursor(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "reset-topic-cursor",
                "Reset the subscription cursor to a specific timestamp",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                        },
                        "subscription": {
                            "type": "string",
                            "description": "The name of the subscription"
                        },
                        "timestamp": {
                            "type": "integer",
                            "description": "The timestamp (in milliseconds) to reset the cursor to",
                            "default": 0
                        }
                    },
                    "required": ["topic", "subscription"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());
                        String subscription = getRequiredStringParam(request.arguments(), "subscription");
                        Long timestamp = getLongParam(request.arguments(), "timestamp", 0L);

                        if (timestamp == null) {
                            timestamp = 0L;
                        }

                        if (timestamp <= 0L){
                            pulsarAdmin.topics().resetCursor(topic, subscription, 0L);
                        } else {
                            pulsarAdmin.topics().resetCursor(topic, subscription, timestamp);
                        }

                        Map<String, Object> response = new HashMap<>();
                        response.put("topic", topic);
                        response.put("subscription", subscription);
                        response.put("timestamp", timestamp);
                        response.put("reset", true);

                        addTopicBreakdown(response, topic);

                        return createSuccessResult("Cursor reset successfully", response);
                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to reset topic cursor", e);
                        return createErrorResult("Failed to reset topic cursor: " + e.getMessage());
                    }
                }).build());
    }

    private void registerGetTopicInternalStats(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-internal-stats",
                "Get internal stats of a Pulsar topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                        }
                    },
                    "required": ["topic"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());

                        var internalStats = pulsarAdmin.topics().getInternalStats(topic);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("entriesAddedCounter", internalStats.entriesAddedCounter);
                        result.put("numberOfEntries", internalStats.numberOfEntries);
                        result.put("totalSize", internalStats.totalSize);
                        result.put("currentLedgerEntries", internalStats.currentLedgerEntries);
                        result.put("currentLedgerSize", internalStats.currentLedgerSize);
                        result.put("lastLedgerCreatedTimestamp", internalStats.lastLedgerCreatedTimestamp);
                        result.put("lastLedgerCreationFailureTimestamp",
                                internalStats.lastLedgerCreationFailureTimestamp);
                        result.put("waitingCursorCount", internalStats.waitingCursorsCount);
                        result.put("pendingAddEntriesCount", internalStats.pendingAddEntriesCount);

                        if (internalStats.ledgers != null && !internalStats.ledgers.isEmpty()) {
                            result.put("ledgers", internalStats.ledgers);
                            result.put("ledgerCount", internalStats.ledgers.size());
                        }

                        if (internalStats.cursors != null && !internalStats.cursors.isEmpty()) {
                            result.put("cursorCount",  internalStats.cursors.size());
                            Map<String, Object> cursors = new HashMap<>();
                            internalStats.cursors.forEach((name, cursor) -> {
                                Map<String, Object> cursorInfo =  new HashMap<>();
                                cursorInfo.put("markDeletePosition", cursor.markDeletePosition);
                                cursorInfo.put("readPosition", cursor.readPosition);
                                cursorInfo.put("waitingReadOp", cursor.waitingReadOp);
                                cursorInfo.put("pendingReadOps", cursor.pendingReadOps);
                                cursorInfo.put("messagesConsumedCounter",
                                        cursor.messagesConsumedCounter);
                                cursorInfo.put("cursorLedger", cursor.cursorLedger);
                                cursors.put(name, cursorInfo);
                            });
                            result.put("cursors", cursors);
                        }
                        addTopicBreakdown(result, topic);
                        return createSuccessResult("Internal stats retrieved successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid input parameter: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get internal stats", e);
                        return createErrorResult("Failed to get internal stats: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerGetPartitionedMetadata(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-partitioned-metadata",
                "Get partitioned metadata of a Pulsar topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name (simple: 'orders' or full: 'persistent://public/default/orders')"
                        }
                    },
                    "required": ["topic"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());

                        var partitionedMetadata = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);

                        int partitions = (partitionedMetadata == null) ? 0 : partitionedMetadata.partitions;
                        result.put("partitions", partitions);
                        result.put("isPartitioned", partitions > 0);

                        if (partitions > 0) {
                            double msgRateIn = 0.0, msgRateOut = 0.0, msgThroughputIn = 0.0, msgThroughputOut = 0.0;
                            long storageSize = 0L;
                            Map<String, Object> partitionInfo = new HashMap<>();

                            for (int i = 0; i < partitions; i++) {
                                String p = topic + "-partition-" + i;
                                try {
                                    TopicStats s = pulsarAdmin.topics().getStats(p);
                                    if (s == null) {
                                        continue;
                                    }
                                    msgRateIn += s.getMsgRateIn();
                                    msgRateOut += s.getMsgRateOut();
                                    msgThroughputIn += s.getMsgThroughputIn();
                                    msgThroughputOut += s.getMsgThroughputOut();
                                    storageSize += s.getStorageSize();

                                    Map<String, Object> partStats = new HashMap<>();
                                    partStats.put("msgRateIn", s.getMsgRateIn());
                                    partStats.put("msgRateOut", s.getMsgRateOut());
                                    partStats.put("storageSize", s.getStorageSize());
                                    int subCount = (s.getSubscriptions() == null)
                                            ? 0 : s.getSubscriptions().size();
                                    partStats.put("subscriptionCount", subCount);
                                    partitionInfo.put(p, partStats);
                                } catch (Exception ex) {
                                    Map<String, Object> err = new HashMap<>();
                                    err.put("error", "Failed to get stats: " + ex.getMessage());
                                    partitionInfo.put(p, err);
                                }
                            }

                            result.put("msgRateIn", msgRateIn);
                            result.put("msgRateOut", msgRateOut);
                            result.put("msgThroughputIn", msgThroughputIn);
                            result.put("msgThroughputOut", msgThroughputOut);
                            result.put("storageSize", storageSize);
                            result.put("partitionStats", partitionInfo);
                        } else {
                            result.put("message", "Topic is not partitioned");
                        }

                        addTopicBreakdown(result, topic);
                        return createSuccessResult("Partitioned metadata retrieved successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid input parameter: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to get partitioned metadata", e);
                        return createErrorResult("Failed to get partitioned metadata: " + e.getMessage());
                    }
                })
                .build());
    }

}

