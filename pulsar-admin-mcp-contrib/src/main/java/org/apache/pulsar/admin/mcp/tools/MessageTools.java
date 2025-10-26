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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pulsar.admin.mcp.client.PulsarClientManager;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

public class MessageTools extends BasePulsarTools {

    private final PulsarClientManager pulsarClientManager;
    private final ConcurrentMap<String, Producer<byte[]>> producerCache = new ConcurrentHashMap<>();

    public MessageTools(PulsarAdmin pulsarAdmin, PulsarClientManager pulsarClientManager) {
        super(pulsarAdmin);
        this.pulsarClientManager = pulsarClientManager;
    }

    protected PulsarClient getClient() throws Exception {
        return pulsarClientManager.getClient();
    }

    private Producer<byte[]> getOrCreateProducer(String fullTopic) throws Exception {
        return producerCache.computeIfAbsent(fullTopic, t -> {
            try {
                PulsarClient client = getClient();
                if (client == null) {
                    throw new RuntimeException("PulsarClient is not available. "
                            + "Please check your Pulsar connection configuration.");
                }

                if (client.isClosed()) {
                    throw new RuntimeException("PulsarClient is closed. Please restart the MCP server.");
                }

                return client.newProducer()
                        .topic(t)
                        .enableBatching(true)
                        .batchingMaxPublishDelay(5, TimeUnit.MILLISECONDS)
                        .blockIfQueueFull(true)
                        .compressionType(CompressionType.LZ4)
                        .sendTimeout(30, TimeUnit.SECONDS)
                        .create();
            } catch (Exception e) {
                throw new RuntimeException("create producer failed for " + t, e);
            }
        });
    }

    public void registerTools(McpSyncServer mcpServer) {
        registerPeekMessage(mcpServer);
        registerExamineMessages(mcpServer);
        registerSkipAllMessages(mcpServer);
        registerExpireAllMessages(mcpServer);
        registerGetMessageBacklog(mcpServer);
        registerSendMessage(mcpServer);
        registerGetMessageStats(mcpServer);
        registerReceiveMessages(mcpServer);
    }

    private void registerPeekMessage(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "peek-message",
                "Peek messages from a subscription without acknowledging them",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
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
                        int numMessages = getIntParam(request.arguments(), "numMessages", 1);

                        if (numMessages <= 0) {
                            return  createErrorResult("Number of messages must be greater than 0.");
                        }

                        List<Message<byte[]>> messages = pulsarAdmin.topics()
                                .peekMessages(topic, subscriptionName, numMessages);

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
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("messagesCount", messages.size());
                        result.put("messages", messageList);

                        addTopicBreakdown(result, topic);

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
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
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
                    "required": ["topic", "subscriptionName"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String topic = buildFullTopicName(request.arguments());
                        String subscriptionName = getStringParam(request.arguments(), "subscriptionName");
                        int numMessages = getIntParam(request.arguments(), "numMessages", 5);

                        if (numMessages <= 0) {
                            return createErrorResult("Invalid number of messages for examine");
                        }

                        List<Message<byte[] >> messages = pulsarAdmin.topics().peekMessages(
                                topic,
                                subscriptionName,
                                numMessages);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
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
                                    messageInfo.put("payloadBase64",
                                            Base64.getEncoder().encodeToString(message.getData()));
                                    try {
                                        messageInfo.put("textUtf8",
                                                new String(message.getData(), StandardCharsets.UTF_8));
                                    } catch (Exception ignore) {

                                    }
                                    messageInfo.put("producerName", message.getProducerName());
                                    return messageInfo;
                                })
                                .collect(Collectors.toList());


                        result.put("messages", detailedMessages);

                        addTopicBreakdown(result, topic);

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
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Subscription name to skip messages for"
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

                        pulsarAdmin.topics().skipAllMessages(topic, subscriptionName);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName",
                                subscriptionName);
                        result.put("skippedAll", true);

                        addTopicBreakdown(result, topic);

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
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Subscription name whose messages should be expired"
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

                        try {
                            var subs = pulsarAdmin.topics().getSubscriptions(topic);
                            if (subs == null || !subs.contains(subscriptionName)) {
                                return createErrorResult("Subscription not found: " + subscriptionName);
                            }
                        } catch (Exception ignore) {

                        }

                        pulsarAdmin.topics().expireMessages(topic, subscriptionName, 0);

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("expiredAll", true);

                        addTopicBreakdown(result, topic);

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
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Subscription name to check backlog for"
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

                        long backlog = 0L;
                        boolean found = false;

                        var meta = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                        if (meta != null && meta.partitions > 0) {
                            var ps = pulsarAdmin.topics().getPartitionedStats(topic, true);
                            if (ps != null && ps.getPartitions() != null) {
                                for (var partEntry : ps.getPartitions().entrySet()) {
                                    TopicStats ts = partEntry.getValue();
                                    if (ts != null && ts.getSubscriptions() != null) {
                                        var sub = ts.getSubscriptions().get(subscriptionName);
                                        if (sub != null) {
                                            backlog += sub.getMsgBacklog();
                                            found = true;
                                        }
                                    }
                                }
                            }
                        } else {
                            TopicStats stats = pulsarAdmin.topics().getStats(topic);
                            if (stats != null && stats.getSubscriptions() != null) {
                                var sub = stats.getSubscriptions().get(subscriptionName);
                                if (sub != null) {
                                    backlog = sub.getMsgBacklog();
                                    found = true;
                                }
                            }
                        }

                        if (!found) {
                            return createErrorResult("Subscription not found: " + subscriptionName);
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("backlog", backlog);

                        addTopicBreakdown(result, topic);
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

    private void registerGetMessageStats(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "get-message-stats",
                "Get message statistics for a topic or subscription",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Optional subscription name to get stats for a specific subscription"
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
                        String subscriptionName = getStringParam(request.arguments(), "subscriptionName");

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);

                        var meta = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                        if (meta != null && meta.partitions > 0) {
                            var ps = pulsarAdmin.topics().getPartitionedStats(topic, true);
                            if (ps == null) {
                                return createErrorResult("Failed to fetch partitioned stats");
                            }

                            result.put("msgInCounter", ps.getMsgInCounter());
                            result.put("msgOutCounter", ps.getMsgOutCounter());
                            result.put("bytesInCounter", ps.getBytesInCounter());
                            result.put("bytesOutCounter", ps.getBytesOutCounter());

                            if (subscriptionName != null && !subscriptionName.isBlank()) {
                                long backlog = 0L;
                                double rateOut = 0D;
                                double rateRedeliver = 0D;
                                boolean found = false;

                                if (ps.getPartitions() != null) {
                                    for (var partEntry : ps.getPartitions().entrySet()) {
                                        TopicStats ts = partEntry.getValue();
                                        if (ts != null && ts.getSubscriptions() != null) {
                                            var sub = ts.getSubscriptions().get(subscriptionName);
                                            if (sub != null) {
                                                backlog += sub.getMsgBacklog();
                                                rateOut += sub.getMsgRateOut();
                                                rateRedeliver += sub.getMsgRateRedeliver();
                                                found = true;
                                            }
                                        }
                                    }
                                }

                                if (!found) {
                                    return createErrorResult("Subscription not found: " + subscriptionName);
                                }
                                result.put("subscriptionName", subscriptionName);
                                result.put("msgBacklog", backlog);
                                result.put("msgRateOut", rateOut);
                                result.put("msgRateRedeliver", rateRedeliver);
                            }
                        } else {
                            TopicStats stats = pulsarAdmin.topics().getStats(topic);
                            result.put("msgInCounter", stats.getMsgInCounter());
                            result.put("msgOutCounter", stats.getMsgOutCounter());
                            result.put("bytesInCounter", stats.getBytesInCounter());
                            result.put("bytesOutCounter", stats.getBytesOutCounter());

                            if (subscriptionName != null && !subscriptionName.isBlank()) {
                                if (stats.getSubscriptions() == null
                                        || !stats.getSubscriptions().containsKey(subscriptionName)) {
                                    return createErrorResult("Subscription not found: " + subscriptionName);
                                }
                                SubscriptionStats subStats = stats.getSubscriptions().get(subscriptionName);
                                result.put("subscriptionName", subscriptionName);
                                result.put("msgBacklog", subStats.getMsgBacklog());
                                result.put("msgRateOut", subStats.getMsgRateOut());
                                result.put("msgRateRedeliver", subStats.getMsgRateRedeliver());
                            }
                        }

                        addTopicBreakdown(result, topic);
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

    private void registerSendMessage(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "send-message",
                "Send a message to a specified topic",
                """
                {
                  "type": "object",
                  "properties": {
                    "topic": {
                      "type": "string",
                      "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
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
                  "required": ["topic", "message"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String fullTopic = buildFullTopicName(request.arguments());
                        String message   = getRequiredStringParam(request.arguments(), "message");
                        String key       = getStringParam(request.arguments(), "key");

                        Map<String, String> propsSafe = new HashMap<>();
                        Object propsObj = request.arguments().get("properties");
                        if (propsObj instanceof Map<?, ?> raw) {
                            for (var e : raw.entrySet()) {
                                if (e.getKey() != null && e.getValue() != null) {
                                    propsSafe.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
                                }
                            }
                        }

                        Producer<byte[]> producer = getOrCreateProducer(fullTopic);

                        TypedMessageBuilder<byte[]> builder = producer.newMessage()
                                .value(message.getBytes(StandardCharsets.UTF_8));

                        if (key != null && !key.isEmpty()) {
                            builder = builder.key(key);
                        }
                        if (!propsSafe.isEmpty()) {
                            builder = builder.properties(propsSafe);
                        }

                        MessageId msgId = builder.send();

                        return createSuccessResult("Message sent", Map.of(
                                "topic", fullTopic,
                                "messageId", msgId.toString(),
                                "messageContent", message,
                                "bytes", message.getBytes(StandardCharsets.UTF_8).length
                        ));
                    } catch (IllegalArgumentException iae) {
                        return createErrorResult("Invalid arguments: " + iae.getMessage());
                    } catch (Exception e) {
                        return createErrorResult("Failed to send message: " + e.getMessage());
                    }
                }).build());
    }

    private void registerReceiveMessages(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "receive-messages",
                "Receive messages from a Pulsar topic",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
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
                            "description": "Total timeout budget in milliseconds (not per-message)",
                            "minimum": 1000,
                            "default": 5000
                        },
                        "ack": {
                            "type": "boolean",
                            "description": "Acknowledge messages after receiving (default: true)",
                            "default": true
                        },
                        "subscriptionType": {
                            "type": "string",
                            "description": "Consumer subscription type",
                            "enum": ["Exclusive","Shared","Failover","Key_Shared"],
                            "default": "Shared"
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
                        int messageCount = Math.max(1, getIntParam(request.arguments(), "messageCount", 10));

                        if (messageCount > 1000) {
                            return createErrorResult("messageCount too large (max 1000)");
                        }
                        int timeoutMs = Math.max(1000, getIntParam(request.arguments(), "timeoutMs", 5000));
                        if (timeoutMs > 120_000) {
                            return createErrorResult("timeoutMs too large (max 120000)");
                        }
                        boolean ack = getBooleanParam(request.arguments(), "ack", true);
                        String subTypeStr = getStringParam(request.arguments(), "subscriptionType");
                        SubscriptionType subType = SubscriptionType.Shared;
                        if (subTypeStr != null) {
                            try {
                                subType = SubscriptionType.valueOf(subTypeStr.replace('-', '_'));
                            } catch (IllegalArgumentException ignore) {

                            }
                        }

                        try {
                            List<String> subs = pulsarAdmin.topics().getSubscriptions(topic);
                            if (subs == null || !subs.contains(subscriptionName)) {
                                return createErrorResult("Subscription not found: " + subscriptionName);
                            }
                        } catch (Exception e) {
                            LOGGER.warn("Failed to verify subscription existence for {}: {}", topic, e.toString());
                        }

                        List<Map<String, Object>> out = new ArrayList<>();

                        try {
                            PulsarClient client = getClient();
                            if (client == null) {
                                return createErrorResult("Pulsar Client is not available");
                            }

                            int rq = Math.min(Math.max(messageCount, 10), 1000);
                            long deadline = System.nanoTime() + (timeoutMs * 1_000_000L);

                            try (Consumer<byte[]> consumer = client.newConsumer()
                                    .topic(topic)
                                    .subscriptionName(subscriptionName)
                                    .subscriptionType(subType)
                                    .receiverQueueSize(rq)
                                    .subscribe()) {

                                for (int i = 0; i < messageCount; i++) {
                                    long remainMs = Math.max(0L, (deadline - System.nanoTime()) / 1_000_000L);
                                    if (remainMs == 0) {
                                        break;
                                    }

                                    Message<byte[]> msg = consumer.receive((int) Math.min(remainMs, Integer.MAX_VALUE),
                                            TimeUnit.MILLISECONDS);
                                    if (msg == null) {
                                        break;
                                    }

                                    Map<String, Object> m = new HashMap<>();
                                    m.put("messageId", msg.getMessageId().toString());
                                    m.put("key", msg.getKey());
                                    byte[] data = msg.getData();
                                    m.put("dataUtf8", safeToUtf8(data));
                                    m.put("dataBase64", java.util.Base64.getEncoder().encodeToString(data));
                                    m.put("publishTime", msg.getPublishTime());
                                    m.put("eventTime", msg.getEventTime());
                                    m.put("properties", msg.getProperties());
                                    m.put("producerName", msg.getProducerName());
                                    try {
                                        m.put("redeliveryCount", msg.getRedeliveryCount());
                                    } catch (Throwable ignore) {}
                                    m.put("dataSize", data != null ? data.length : 0);

                                    out.add(m);

                                    if (ack) {
                                        try {
                                            consumer.acknowledge(msg);
                                        } catch (Exception ackEx) {
                                            LOGGER.warn("Acknowledge failed: {}", ackEx.toString());
                                        }
                                    }
                                }
                            }
                        } catch (Exception clientException) {
                            LOGGER.error("Failed to receive messages using PulsarClient", clientException);
                            return createErrorResult("Failed to receive messages - PulsarClient error: "
                                    + clientException.getMessage());
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("requestCount", messageCount);
                        result.put("receivedCount", out.size());
                        result.put("ack", ack);
                        result.put("subscriptionType", subType.toString());
                        result.put("timeoutMs", timeoutMs);
                        result.put("messages", out);

                        addTopicBreakdown(result, topic);
                        return createSuccessResult("Messages received successfully", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult("Invalid input parameter: " + e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Unexpected error while receiving messages", e);
                        return createErrorResult("Unexpected error: " + e.getMessage());
                    }
                })
                .build());
    }

    private static String safeToUtf8(byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return new String(data, StandardCharsets.UTF_8);
        } catch (Throwable ignore) {
            return null;
        }
    }

}
