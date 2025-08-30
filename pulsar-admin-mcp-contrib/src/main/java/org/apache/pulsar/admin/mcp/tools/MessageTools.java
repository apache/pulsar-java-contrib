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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

public class MessageTools extends BasePulsarTools {

    private final PulsarClient pulsarClient;

    public MessageTools(PulsarAdmin pulsarAdmin) {
        super(pulsarAdmin);
        this.pulsarClient = null;
    }

    private boolean hasClientSupport() {
        return pulsarClient != null;
    }

    public void registerTools(McpSyncServer mcpServer) {
        registerPeekMessage(mcpServer);
        registerExamineMessages(mcpServer);
        registerSkipAllMessages(mcpServer);
        registerExpireAllMessages(mcpServer);
        registerGetMessageBacklog(mcpServer);
        registerSendMessage(mcpServer);
        registerGetMessageStats(mcpServer);

        if (hasClientSupport()) {
            registerReceiveMessages(mcpServer);
        }
    }

    private void registerPeekMessage(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "peek-message",
                "Peek messages from a subscription without acknowledging them",
                """
                {
                    "type": "object",
                    "properties": {
                        "topicName": {
                            "type": "string",
                            "description": "Topic name (can be simple name like 'orders' or"
                                            + "full name like 'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "The name of the subscription to peek messages from"
                        },
                        "numMessages": {
                            "type": "integer",
                            "description": "Number of messages to peek (default: 1)",
                            "minimum": 1,
                            "default": 1
                        }
                    },
                    "required": ["topicName", "subscriptionName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topicName = buildFullTopicName(request.arguments());
                        String subscriptionName = getRequiredStringParam(request.arguments(), "subscriptionName");
                        int numMessages = getIntParam(request.arguments(), "numMessages", 1);

                        if (numMessages <= 0) {
                            return  createErrorResult("Number of messages must be greater than 0.");
                        }

                        List<Message<byte[]>> messages = pulsarAdmin.topics()
                                .peekMessages(topicName, subscriptionName, numMessages);

                        List<Map<String, Object>> messageList = new ArrayList<>();
                        for (Message<byte[]> msg : messages) {
                            Map<String, Object> msgInfo = new HashMap<>();
                            msgInfo.put("messageId", msg.getMessageId().toString());
                            msgInfo.put("properties", msg.getProperties());
                            msgInfo.put("publishTime", msg.getPublishTime());
                            msgInfo.put("data", new String(msg.getData()));
                            messageList.add(msgInfo);
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topicName", topicName);
                        result.put("subscriptionName", subscriptionName);
                        result.put("messagesCount", messages.size());
                        result.put("messages", messageList);

                        addTopicBreakdown(result, topicName);

                        return createSuccessResult("Peeked " + messageList.size() + " message(s) successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to peek messages", e);
                        return createErrorResult("Failed to peek messages: " + e.getMessage());
                    }
                }).build()
        );
    }

    private void registerExamineMessages(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "examine-messages",
                "Examine messages from a topic without consuming them",
                """
                {
                    "type": "object",
                    "properties": {
                        "topicName": {
                            "type": "string",
                            "description": "Topic name (can be simple name like 'orders' or"
                                            + "full name like 'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "The name of the subscription to peek messages from"
                        },
                        "numMessages": {
                            "type": "integer",
                            "description": "Number of messages to examine",
                            "minimum": 1,
                            "default": 5
                        }
                    },
                    "required": ["topicName", "subscriptionName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    Reader<byte[]> reader = null;
                    try {
                        String topicName = buildFullTopicName(request.arguments());
                        String subscriptionName = getStringParam(request.arguments(), "subscriptionName");
                        int numMessages = getIntParam(request.arguments(), "numMessages", 5);

                        if (numMessages <= 0) {
                            return createErrorResult("Invalid number of messages for examine");
                        }

                        List<Message<byte[] >> messages = pulsarAdmin.topics().peekMessages(
                                topicName,
                                subscriptionName,
                                numMessages);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topicName", topicName);
                        result.put("subscriptionName", subscriptionName);
                        result.put("messageCount", messages.size());

                        List<Map<String, Object>> detailedMessages = messages.stream()
                                .map(message -> {
                                    Map<String, Object> messageInfo = new HashMap<>();
                                    messageInfo.put("messageId", message.getMessageId().toString());
                                    messageInfo.put("properties", message.getProperties());
                                    messageInfo.put("eventTime", message.getEventTime());
                                    messageInfo.put("key", message.getKey());
                                    messageInfo.put("publishTime", message.getPublishTime());
                                    messageInfo.put("data", new String(message.getData(), StandardCharsets.UTF_8));
                                    messageInfo.put("producerName", message.getProducerName());

                                    return messageInfo;
                                })
                                .toList();

                        result.put("messages", detailedMessages);

                        addTopicBreakdown(result, topicName);

                        return createSuccessResult("Examined messages successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to examine messages", e);
                        return createErrorResult("Failed to examine messages: " + e.getMessage());
                    }
                }).build()
        );
    }

    private void registerSkipAllMessages(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "skip-all-messages",
                "Skip all messages in a subscription (set cursor to latest)",
                """
                {
                    "type": "object",
                    "properties": {
                        "topicName": {
                            "type": "string",
                            "description": "Topic name (can be simple name like 'orders' or"
                                            + "full name like 'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Subscription name to skip messages for"
                        }
                    },
                    "required": ["topicName", "subscriptionName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topicName = buildFullTopicName(request.arguments());
                        String subscriptionName = getRequiredStringParam(request.arguments(), "subscriptionName");

                        pulsarAdmin.topics().skipAllMessages(topicName, subscriptionName);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topicName", topicName);
                        result.put("subscriptionName",
                                subscriptionName);
                        result.put("skippedAll", true);

                        addTopicBreakdown(result, topicName);

                        return createSuccessResult("Skipped all messages for subscription: "
                                + subscriptionName, result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Unexpected error while skipping messages", e);
                        return createErrorResult("Unexpected error: " + e.getMessage());
                    }
                }).build()
        );
    }

    private void registerExpireAllMessages(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "expire-all-messages",
                "Expire all messages in a subscription immediately",
                """
                {
                    "type": "object",
                    "properties": {
                        "topicName": {
                            "type": "string",
                            "description": "Topic name (can be simple name like 'orders' or"
                                            + "full name like 'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Subscription name whose messages should be expired"
                        }
                    },
                    "required": ["topicName", "subscriptionName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topicName = buildFullTopicName(request.arguments());
                        String subscriptionName = getRequiredStringParam(request.arguments(), "subscriptionName");

                        pulsarAdmin.topics().expireMessages(topicName, subscriptionName, 0);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topicName",
                                topicName);
                        result.put("subscriptionName", subscriptionName);
                        result.put("expiredAll", true);

                        addTopicBreakdown(result, topicName);

                        return createSuccessResult("Expired all messages for subscription: "
                                + subscriptionName, result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Unexpected error while expiring messages", e);
                        return createErrorResult("Unexpected error: " + e.getMessage());
                    }
                }).build()
        );
    }

    private void registerGetMessageBacklog(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-message-backlog",
                "Get the current backlog message count for a subscription",
                """
                {
                    "type": "object",
                    "properties": {
                        "topicName": {
                            "type": "string",
                            "description": "Topic name (can be simple name like 'orders' or"
                                            + "full name like 'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Subscription name to check backlog for"
                        }
                    },
                    "required": ["topicName", "subscriptionName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topicName = buildFullTopicName(request.arguments());
                        String subscriptionName = getRequiredStringParam(request.arguments(), "subscriptionName");

                        long backlog = pulsarAdmin.topics()
                                .getStats(topicName)
                                .getSubscriptions()
                                .get(subscriptionName)
                                .getMsgBacklog();

                        Map<String, Object> result = new HashMap<>();
                        result.put("topicName", topicName);
                        result.put("subscriptionName", subscriptionName);
                        result.put("backlog", backlog);

                        addTopicBreakdown(result, topicName);

                        return createSuccessResult("Backlog retrieved successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Unexpected error while getting message backlog", e);
                        return createErrorResult("Unexpected error: " + e.getMessage());
                    }
                }).build()
        );
    }

    private void registerSendMessage(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "send-message",
                "Send a message to a specified topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topicName": {
                            "type": "string",
                            "description": "Topic name (can be simple name like 'orders' or"
                                            + "full name like 'persistent://public/default/orders')"
                        },
                        "message": {
                            "type": "string",
                            "description": "The message content to send"
                        },
                        "key": {
                            "type": "string",
                            "description": "Optional message key"
                        },
                        "properties": {
                            "type": "object",
                            "description": "Optional key-value properties for the message"
                        }
                    },
                    "required": ["topicName", "message"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topicName = buildFullTopicName(request.arguments());
                        String message = getRequiredStringParam(request.arguments(), "message");
                        String key = getStringParam(request.arguments(), "key");

                        if (hasClientSupport()) {
                            return sendMessageWithClient(topicName, message, key, request.arguments());
                        } else {
                            Map<String, Object> result = new HashMap<>();
                            result.put("topicName", topicName);
                            result.put("messageId", message);
                            result.put("messageSize", message.getBytes().length);
                            result.put("status", "not_implemented");
                            result.put("reason", "Message sending requires PulsarClient producer");
                            if (key != null) {
                                result.put("key", key);
                            }

                            addTopicBreakdown(result, topicName);

                            return createSuccessResult("Message sent successfully to topic: "
                                    + topicName, result);
                        }

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Unexpected error while sending message", e);
                        return createErrorResult("Unexpected error: " + e.getMessage());
                    }
                }).build()
        );
    }

    private McpSchema.CallToolResult sendMessageWithClient(String topicName, String messageContent,
                                                           String key, Map<String, Object> arguments) {
        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(true)
                .sendTimeout(30, TimeUnit.SECONDS);

        try (Producer<byte[]> producer = producerBuilder.create()) {
            TypedMessageBuilder<byte[]> msgBuilder = producer.newMessage()
                    .value(messageContent.getBytes(StandardCharsets.UTF_8));

            if (key != null && !key.isEmpty()) {
                msgBuilder.key(key);
            }

            Object propertiesObj = arguments.get("properties");
            if (propertiesObj instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> propMap = (Map<String, Object>) propertiesObj;
                Map<String, String> stringProps = new HashMap<>();
                propMap.forEach((k, v) -> {
                    if (v != null) {
                        stringProps.put(k, v.toString());
                    }
                });
                if (!stringProps.isEmpty()) {
                    msgBuilder.properties(stringProps);
                }
            }

            MessageId messageId = msgBuilder.send();

            Map<String, Object> result = new HashMap<>();
            result.put("topicName", topicName);
            result.put("messageId", messageId.toString());
            result.put("message", messageContent);
            if (key != null && !key.isEmpty()) {
                result.put("key", key);
            }
            if (propertiesObj != null) {
                result.put("properties", propertiesObj);
            }

            addTopicBreakdown(result, topicName);

            return createSuccessResult("Message sent successfully to topic: " + topicName, result);

        } catch (PulsarClientException e) {
            LOGGER.error("Failed to send message", e);
            return createErrorResult("PulsarClientException: " + e.getMessage());
        } catch (Exception e) {
            LOGGER.error("Unexpected error while sending message", e);
            return createErrorResult("Unexpected error: " + e.getMessage());
        }
    }

    private void registerGetMessageStats(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-message-stats",
                "Get message statistics for a topic or subscription",
                """
                {
                    "type": "object",
                    "properties": {
                        "topicName": {
                            "type": "string",
                            "description": "Topic name (can be simple name like 'orders' or"
                                            + "full name like 'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Optional subscription name to get stats for a specific subscription"
                        }
                    },
                    "required": ["topicName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topicName = buildFullTopicName(request.arguments());
                        String subscriptionName = getStringParam(request.arguments(), "subscriptionName");

                        TopicStats stats = pulsarAdmin.topics().getStats(topicName);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topicName", topicName);
                        result.put("msgInCounter", stats.getMsgInCounter());
                        result.put("msgOutCounter", stats.getMsgOutCounter());
                        result.put("bytesInCounter", stats.getBytesInCounter());
                        result.put("bytesOutCounter", stats.getBytesOutCounter());

                        if (subscriptionName != null && stats.getSubscriptions().containsKey(subscriptionName)) {
                            SubscriptionStats subStats = stats.getSubscriptions().get(subscriptionName);
                            result.put("subscriptionName", subscriptionName);
                            result.put("msgBacklog", subStats.getMsgBacklog());
                            result.put("msgRateOut", subStats.getMsgRateOut());
                            result.put("msgRateRedeliver", subStats.getMsgRateRedeliver());
                        }

                        addTopicBreakdown(result, topicName);

                        return createSuccessResult("Fetched message stats successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Unexpected error while getting message stats", e);
                        return createErrorResult("Unexpected error: " + e.getMessage());
                    }
                }).build()
        );
    }

    private void registerReceiveMessages(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "receive-messages",
                "Receive messages from a Pulsar topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topicName": {
                            "type": "string",
                            "description": "Topic name (can be simple name like 'orders' or"
                                            + "full name like 'persistent://public/default/orders')"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Subscription name for the consumer"
                        },
                        "messageCount": {
                            "type": "integer",
                            "description": "Number of messages to receive",
                            "minimum": 1,
                            "default": 10
                        },
                        "timeoutMs": {
                            "type": "integer",
                            "description": "Timeout in milliseconds for receiving messages",
                            "minimum": 1000,
                            "default": 5000
                        }
                    },
                    "required": ["topicName", "subscriptionName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = getRequiredStringParam(request.arguments(), "topic");
                        String subscriptionName = getRequiredStringParam(request.arguments(), "subscriptionName");
                        Integer messageCount = getIntParam(request.arguments(), "messageCount", 10);
                        Integer timeoutMs = getIntParam(request.arguments(), "timeoutMs", 5000);

                        List<Map<String, Object>> messages = new ArrayList<>();

                        if (pulsarClient != null) {
                            try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                                    .topic(topic)
                                    .subscriptionName(subscriptionName)
                                    .subscriptionType(SubscriptionType.Exclusive)
                                    .subscribe()) {


                                for (int i = 0; i < messageCount; i++) {
                                    Message<byte[]> msg = consumer.receive(timeoutMs, TimeUnit.MILLISECONDS);
                                    if (msg == null) {
                                        break;
                                    }

                                    Map<String, Object> messageInfo = new HashMap<>();
                                    messageInfo.put("messageId", msg.getMessageId().toString());
                                    messageInfo.put("key", msg.getKey());
                                    messageInfo.put("content", new String(msg.getData(), StandardCharsets.UTF_8));
                                    messageInfo.put("publishTime", msg.getPublishTime());
                                    messageInfo.put("properties", msg.getProperties());
                                    messageInfo.put("dataSize", msg.getData().length);

                                    messages.add(messageInfo);
                                    consumer.acknowledge(msg);
                                }
                            }
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topicName", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("requestCount", messageCount);
                        result.put("receivedCount", messages.size());
                        result.put("messages", messages);
                        result.put("timeoutMs", timeoutMs);

                        addTopicBreakdown(result, topic);

                        return createSuccessResult("Messages received successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid input parameter: " + e.getMessage());
                    } catch (PulsarClientException e) {
                        LOGGER.error("Failed to receive messages", e);
                        return createErrorResult("Failed to receive messages: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Unexpected error while receiving messages", e);
                        return createErrorResult("Unexpected error: " + e.getMessage());
                    }
                })
                .build());
    }

}
