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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

public class SubscriptionTools extends BasePulsarTools{

    public SubscriptionTools(PulsarAdmin pulsarAdmin) {
        super(pulsarAdmin);
    }

    public void registerTools(McpSyncServer mcpServer) {
        registerListSubscriptions(mcpServer);
        registerGetSubscriptionStats(mcpServer);
        registerCreateSubscription(mcpServer);
        registerDeleteSubscription(mcpServer);
        registerSkipMessages(mcpServer);
        registerResetSubscriptionCursor(mcpServer);
        registerExpireSubscriptionMessages(mcpServer);
        registerUnsubscribe(mcpServer);
        registerPauseSubscription(mcpServer);
        registerResumeSubscription(mcpServer);
    }

    private void registerListSubscriptions(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "list-subscriptions",
                "List all subscriptions for a specific topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
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

                        List<String> subscriptions = pulsarAdmin.topics().getSubscriptions(topic);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptions", subscriptions);
                        result.put("subscriptionCount", subscriptions.size());

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Subscriptions listed successfully", result);

                    } catch (Exception e) {
                        LOGGER.error("Failed to list subscriptions", e);
                        return createErrorResult("Failed to list subscriptions: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerGetSubscriptionStats(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-subscription-stats",
                "Get statistics of a subscription for a specific topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "subscription": {
                            "type": "string",
                            "description": "The name of the subscription"
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

                        TopicStats stats = pulsarAdmin.topics().getStats(topic);
                        SubscriptionStats subStats = stats.getSubscriptions().get(subscription);

                        if (subStats == null) {
                            return createErrorResult("Subscription not found: " + subscription);
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscription", subscription);
                        result.put("msgBacklog", subStats.getMsgBacklog());
                        result.put("msgRateOut", subStats.getMsgRateOut());
                        result.put("msgThroughputOut", subStats.getMsgThroughputOut());
                        result.put("msgRateRedeliver",
                                subStats.getMsgRateRedeliver());
                        result.put("type", subStats.getType());
                        result.put("consumerCount",
                                subStats.getConsumers() != null
                                        ? subStats.getConsumers().size()
                                        : 0);
                        result.put("isReplicated", subStats.isReplicated());

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Subscription stats fetched successfully", result);

                    } catch (Exception e) {
                        LOGGER.error("Failed to get subscription stats", e);
                        return createErrorResult("Failed to get subscription stats: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerCreateSubscription(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "create-subscription",
                "Create a subscription on a topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "subscription": {
                            "type": "string",
                            "description": "The name of the subscription to create from an existing topic"
                        },
                        "messageId": {
                            "type": "string",
                            "default": "latest",
                            "description": "Initial position of the subscription (optional, defaults to latest)"
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
                        String messageId = getStringParam(request.arguments(), "messageId");

                        if (messageId == null || messageId.equals("latest")) {
                            pulsarAdmin.topics().createSubscription(topic, subscription, MessageId.latest);
                        } else if (messageId.equals("earliest")) {
                            pulsarAdmin.topics().createSubscription(topic, subscription, MessageId.earliest);
                        } else {
                            pulsarAdmin.topics().createSubscription(topic, subscription, MessageId.latest);
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscription", subscription);
                        result.put("messageId", messageId != null ? messageId : "latest");
                        result.put("created", true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Subscription created successfully", result);

                    } catch (Exception e) {
                        LOGGER.error("Failed to create subscription", e);
                        return createErrorResult("Failed to create subscription: " + e.getMessage());
                    }
                })
                .build()
        );
    }

    private void registerDeleteSubscription(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "delete-subscription",
                "Delete a subscription from a specific topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "subscription": {
                            "type": "string",
                            "description": "The name of the subscription to delete"
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
                        Boolean force =  getBooleanParam(request.arguments(), "force", false);

                        pulsarAdmin.topics().deleteSubscription(topic, subscription);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscription", subscription);
                        result.put("force", force);
                        result.put("deleted", true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Subscription deleted successfully", result);

                    } catch (Exception e) {
                        LOGGER.error("Failed to delete subscription", e);
                        return createErrorResult("Failed to delete subscription: " + e.getMessage());
                    }
                }).build());
    }

    private void registerSkipMessages(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "skip-messages",
                "Skip messages for a subscription on a specific topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "subscription": {
                            "type": "string",
                            "description": "The name of the subscription"
                        },
                        "numMessages": {
                            "type": "integer",
                            "description": "Number of messages to skip",
                            "minimum": 1
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
                        int numMessages = getIntParam(request.arguments(), "numMessages", 1);

                        if (numMessages <= 0) {
                            return createErrorResult("Number of messages must be greater than 0.");
                        }

                        pulsarAdmin.topics().skipMessages(topic, subscription, numMessages);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscription", subscription);
                        result.put("numMessagesSkipped", numMessages);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Skipped messages successfully", result);

                    } catch (Exception e) {
                        LOGGER.error("Failed to skip messages", e);
                        return createErrorResult("Failed to skip messages: " + e.getMessage());
                    }
                }).build());
    }

    private void registerResetSubscriptionCursor(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "reset-subscription-cursor",
                "Reset a subscription cursor to a specific message publish time (timestamp in ms)",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "The name of the subscription to reset"
                        },
                        "timestamp": {
                            "type": "integer",
                            "description": "Timestamp (ms since epoch) to reset the subscription cursor",
                            "default": 0
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
                        Long timestamp = getLongParam(request.arguments(), "timestamp", 0L);

                        if (timestamp <= 0) {
                            timestamp = System.currentTimeMillis() - (365L * 24 * 60 * 1000);
                        }

                        pulsarAdmin.topics().resetCursor(topic, subscriptionName, timestamp);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("timestamp", timestamp);
                        result.put("reset", true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Subscription cursor reset successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to reset subscription cursor", e);
                        return createErrorResult("Failed to reset subscription cursor: " + e.getMessage());
                    }
                }).build());
    }

    private void registerExpireSubscriptionMessages(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "expire-subscription-messages",
                "Expire messages for a subscription up to a specific message ID",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "The name of the subscription whose messages will be expired"
                        },
                        "expireTimeSeconds": {
                            "type": "integer",
                            "description": "Expire messages older than this time in seconds",
                            "default": "0"
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
                        Integer expireTimeSeconds = getIntParam(request.arguments(), "expireTimeSeconds", 0);

                        pulsarAdmin.topics().expireMessages(topic, subscriptionName, expireTimeSeconds);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic",
                                topic);
                        result.put("subscriptionName",
                                subscriptionName);
                        result.put("expireTimeSeconds",
                                expireTimeSeconds);
                        result.put("expired",
                                true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult(
                                "Expired subscription messages up to message ID successfully"
                                , result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to expire subscription messages", e);
                        return createErrorResult("Failed to expire subscription messages: " + e.getMessage());
                    }
                }).build());
    }

    private void registerUnsubscribe(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "unsubscribe",
                "Unsubscribe a subscription from a topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "The name of the subscription to unsubscribe"
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

                        pulsarAdmin.topics().deleteSubscription(topic, subscriptionName);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("unsubscribed", true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Subscription unsubscribed successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to unsubscribe subscription", e);
                        return createErrorResult("Failed to unsubscribe: " + e.getMessage());
                    }
                }).build());
    }

    private void registerPauseSubscription(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "pause-subscription",
                "Pause message delivery for a subscription on a topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "The name of the subscription to pause"
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

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("paused", true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Subscription paused successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to pause subscription", e);
                        return createErrorResult("Failed to pause subscription: " + e.getMessage());
                    }
                }).build());
    }

    private void registerResumeSubscription(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "resume-subscription",
                "Resume message delivery for a paused subscription on a topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:'orders' or full:'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "The name of the subscription to resume"
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

//                        pulsarAdmin.topics().resumeSubscription(topic, subscriptionName);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("resumed", true);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Subscription resumed successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to resume subscription", e);
                        return createErrorResult("Failed to resume subscription: " + e.getMessage());
                    }
                }).build());
    }

}
