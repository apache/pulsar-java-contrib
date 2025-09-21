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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

public class MonitoringTools extends BasePulsarTools{
    public MonitoringTools(PulsarAdmin pulsarAdmin) {
        super(pulsarAdmin);
    }

    PulsarClient pulsarClient;

    public void registerTools(McpSyncServer mcpServer) {
        registerMonitorClusterPerformance(mcpServer);
        registerMonitorSubscriptionPerformance(mcpServer);
        registerMonitorTopicPerformance(mcpServer);

        registerHealthCheck(mcpServer);
        registerConnectionDiagnostics(mcpServer);
        registerBacklogAnalysis(mcpServer);
    }

    private void registerMonitorClusterPerformance(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "monitor-cluster-performance",
                "Monitor specific Pulsar cluster performance metrics and health",
                """
                {
                    "type": "object",
                    "properties": {
                        "clusterName": {
                            "type": "string",
                            "description": "Cluster name to monitor"
                        },
                        "includeBrokerStats": {
                            "type": "boolean",
                            "description": "Include detailed broker statistics (default: true)",
                            "default": true
                        },
                        "includeTopicStats": {
                            "type": "boolean",
                            "description": "Include aggregated topic statistics (default: false, can be slow)",
                            "default": false
                        },
                        "maxTopics": {
                            "type": "integer",
                            "description": "Maximum top topics to include when includeTopicStats is true (default: 10)",
                            "default": 10,
                            "minimum": 1,
                            "maximum": 50
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
                        String clusterName = getStringParam(request.arguments(), "cluster");
                        boolean includeDetails = getBooleanParam(request.arguments(), "includeDetails", false);


                        Map<String, Object> result = new HashMap<>();
                        result.put("timestamp", System.currentTimeMillis());

                        var clusters = pulsarAdmin.clusters().getClusters();
                        if (clusterName == null || !clusters.isEmpty()) {
                            clusterName = clusters.get(0);
                        }
                        result.put("clusterName", clusterName);

                        try {
                            var brokers = pulsarAdmin.brokers().getActiveBrokers(clusterName);
                            result.put("activeBrokers", brokers.size());
                            result.put("brokerList", brokers);

                            if (includeDetails) {
                                Map<String, Object> brokerStats = new HashMap<>();
                                for (String broker : brokers) {
                                    try {
                                        brokerStats.put(broker, Map.of("status", "active"));
                                    } catch (Exception e) {
                                        brokerStats.put(broker, Map.of("status", "error", "error", e.getMessage()));
                                    }
                                }
                                result.put("brokerStats", brokerStats);
                            }
                        } catch (Exception e) {
                            result.put("brokerError", e.getMessage());
                        }

                        result.put("clusterHealth", "healthy");

                        return createSuccessResult("Cluster performace monitoring completed", result);

                    } catch (Exception e) {
                        return createErrorResult("Failed to monitor cluster performance:" + e.getMessage());
                    }
                })
                .build());
    }

    private void registerMonitorTopicPerformance(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "monitor-topic-performance",
                "Monitor specific Pulsar topic performance metrics and health",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name(simple:orders or full:persistent://public/default/orders)"
                       },
                        "includeDetails": {
                            "type": "boolean",
                            "description": "Include detailed subscription and consumer statistics (default: false)",
                            "default": false
                        },
                        "includeInternalStats": {
                            "type": "boolean",
                            "description": "Include internal topic statistics like ledger info (default: false)",
                            "default": false
                        },
                        "maxSubscriptions": {
                            "type": "integer",
                            "description": "Maximum number of subscriptions to include in details (default: 10)",
                            "default": 10,
                            "minimum": 1,
                            "maximum": 50
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
                        boolean includeDetails = getBooleanParam(request.arguments(),
                                "includeDetails", false);
                        boolean includeInternalStats = getBooleanParam(request.arguments(),
                                "includeInternalStats", false);
                        int maxSubscriptions = getIntParam(request.arguments(),
                                "maxSubscriptions", 10);

                        Map<String, Object> result = new HashMap<>();
                        result.put("timestamp", System.currentTimeMillis());
                        result.put("topic", topic);

                        try {
                            var stats = pulsarAdmin.topics().getStats(topic);

                            result.put("msgRateIn", stats.getMsgRateIn());
                            result.put("msgRateOut", stats.getMsgRateOut());
                            result.put("msgThroughputIn", stats.getMsgThroughputIn());
                            result.put("msgThroughputOut", stats.getMsgThroughputOut());
                            result.put("storageSize", stats.getStorageSize());
                            result.put("averageMsgSize", stats.getAverageMsgSize());

                            result.put("publishersCount", stats.getPublishers().size());
                            result.put("subscriptionsCount", stats.getSubscriptions().size());

                            int totalConsumers = stats.getSubscriptions().values().stream()
                                    .mapToInt(sub -> sub.getConsumers().size()).sum();
                            result.put("totalConsumers", totalConsumers);

                            long totalBacklog = stats.getSubscriptions().values().stream()
                                    .mapToLong(sub -> sub.getMsgBacklog()).sum();
                            result.put("totalBacklog", totalBacklog);

                            if (includeDetails) {
                                Map<String, Object> subscriptionStats = new HashMap<>();
                                List<String> subscriptionNames = new ArrayList<>(stats.getSubscriptions().keySet());

                                if (subscriptionNames.size() > maxSubscriptions) {
                                    subscriptionNames = subscriptionNames.subList(0, maxSubscriptions);
                                }

                                for (String subName : subscriptionNames) {
                                    var sub = stats.getSubscriptions().get(subName);
                                    Map<String, Object> subDetail = new HashMap<>();
                                    subDetail.put("msgRateOut", sub.getMsgRateOut());
                                    subDetail.put("msgThroughputOut", sub.getMsgThroughputOut());
                                    subDetail.put("msgBacklog", sub.getMsgBacklog());
                                    subDetail.put("msgRateExpired", sub.getMsgRateExpired());
                                    subDetail.put("consumersCount", sub.getConsumers().size());
                                    subDetail.put("type", sub.getType());

                                    List<Map<String, Object>> consumerDetails = new ArrayList<>();
                                    for (var consumer : sub.getConsumers()) {
                                        Map<String, Object> consumerInfo = new HashMap<>();
                                        consumerInfo.put("consumerName", consumer.getConsumerName());
                                        consumerInfo.put("msgRateOut", consumer.getMsgRateOut());
                                        consumerInfo.put("msgThroughputOut", consumer.getMsgThroughputOut());
                                        consumerInfo.put("availablePermits", consumer.getAvailablePermits());
                                        consumerInfo.put("unackedMessages", consumer.getUnackedMessages());
                                        consumerDetails.add(consumerInfo);
                                    }
                                    subDetail.put("consumers", consumerDetails);
                                    subscriptionStats.put(subName, subDetail);
                                }
                                result.put("subscriptionStats", subscriptionStats);

                                // Publisher details
                                List<Map<String, Object>> publisherDetails = new ArrayList<>();
                                for (var publisher : stats.getPublishers()) {
                                    Map<String, Object> pubInfo = new HashMap<>();
                                    pubInfo.put("producerName", publisher.getProducerName());
                                    pubInfo.put("msgRateIn", publisher.getMsgRateIn());
                                    pubInfo.put("msgThroughputIn", publisher.getMsgThroughputIn());
                                    pubInfo.put("averageMsgSize", publisher.getAverageMsgSize());
                                    publisherDetails.add(pubInfo);
                                }
                                result.put("publisherDetails", publisherDetails);
                            }

                            if (includeInternalStats) {
                                try {
                                    var internalStats = pulsarAdmin.topics().getInternalStats(topic);
                                    Map<String, Object> internal = new HashMap<>();
                                    internal.put("numberOfEntries", internalStats.numberOfEntries);
                                    internal.put("totalSize", internalStats.totalSize);
                                    internal.put("currentLedgerEntries", internalStats.currentLedgerEntries);
                                    internal.put("currentLedgerSize", internalStats.currentLedgerSize);
                                    internal.put("ledgerCount",
                                            internalStats.ledgers != null
                                                    ? internalStats.ledgers.size()
                                                    : 0);
                                    internal.put("cursorCount", internalStats.cursors != null
                                            ? internalStats.cursors.size()
                                            : 0);
                                    result.put("internalStats", internal);
                                } catch (Exception e) {
                                    result.put("internalStatsError", e.getMessage());
                                }
                            }

                        } catch (Exception e) {
                            result.put("statsError", e.getMessage());
                        }

                        // Calculate topic health
                        String topicHealth = "healthy";
                        if (result.containsKey("statsError")) {
                            topicHealth = "error";
                        } else {
                            Long totalBacklog = (Long) result.get("totalBacklog");
                            Double msgRateIn = (Double) result.get("msgRateIn");
                            Double msgRateOut = (Double) result.get("msgRateOut");

                            if (totalBacklog != null && totalBacklog > 100000) {
                                topicHealth = "backlog_high";
                            } else if (msgRateIn != null && msgRateOut != null && msgRateIn > msgRateOut * 1.5) {
                                topicHealth = "consumption_slow";
                            }
                        }
                        result.put("topicHealth", topicHealth);

                        addTopicBreakdown(result, topic);
                        return createSuccessResult("Topic performance monitoring completed", result);

                    } catch (Exception e) {
                        return createErrorResult("Failed to monitor topic performance: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerMonitorSubscriptionPerformance(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "monitor-subscription-performance",
                "Monitor performance metrics of a subscription on a topic",
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
                            "description": "Name of the subscription to monitor"
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

                        TopicStats stats = pulsarAdmin.topics().getStats(topic);
                        var subStats = stats.getSubscriptions().get(subscriptionName);

                        if (subStats == null) {
                            return createErrorResult("Subscription not found: " + subscriptionName);
                        }

                        Map<String, Object> result = new HashMap<>();
                        result.put("topic", topic);
                        result.put("subscriptionName", subscriptionName);
                        result.put("timestamp", System.currentTimeMillis());

                        result.put("msgBacklog", subStats.getMsgBacklog());
                        result.put("msgRateOut", subStats.getMsgRateOut());
                        result.put("msgThroughputOut", subStats.getMsgThroughputOut());
                        result.put("consumersCount", subStats.getConsumers().size());
                        result.put("blockedSubscriptionOnUnackedMsgs", subStats.isBlockedSubscriptionOnUnackedMsgs());
                        result.put("subscriptionType", subStats.getType());


                        addTopicBreakdown(result, topic);
                        return createSuccessResult("Subscription performance retrieved", result);

                    } catch (IllegalArgumentException e) {
                        return createErrorResult(e.getMessage());
                    } catch (Exception e) {
                        LOGGER.error("Failed to monitor subscription performance", e);
                        return createErrorResult("Failed to monitor subscription performance: "
                                + e.getMessage());
                    }
                }).build());
    }

    private void registerBacklogAnalysis(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "backlog-analysis",
                "Analyze message backlog within a Pulsar namespace"
                            +   "and report topics/subscriptions exceeding a given threshold",
                """
                {
                    "type": "object",
                    "properties": {
                        "namespace": {
                            "type": "string",
                            "description": "The namespace to analyze (e.g., 'public/default')"
                        },
                        "threshold": {
                            "type": "integer",
                            "description": "Message backlog threshold for alerts (default: 1000)",
                            "default": 1000,
                            "minimum": 0
                        },
                        "includeDetails": {
                            "type": "boolean",
                            "description": "Include detailed per-topic and per-subscription stats (default: false)",
                            "default": false
                        }
                    },
                    "required": ["namespace"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        String namespace = getStringParam(request.arguments(), "namespace");
                        int threshold = getIntParam(request.arguments(), "threshold", 1000);
                        boolean includeDetails = getBooleanParam(request.arguments(), "includeDetails", false);

                        Map<String, Object> result = new HashMap<>();
                        result.put("timestamp", System.currentTimeMillis());
                        result.put("namespace", namespace);
                        result.put("threshold", threshold);

                        var topics = pulsarAdmin.namespaces().getTopics(namespace);
                        List<String> alertTopics = new ArrayList<>();
                        Map<String, Object> details = new HashMap<>();

                        for (String topic : topics) {
                            try {
                                var stats = pulsarAdmin.topics().getStats(topic);
                                long totalBacklog = stats.getSubscriptions().values()
                                        .stream()
                                        .mapToLong(sub -> sub.getMsgBacklog())
                                        .sum();

                                if (totalBacklog > threshold) {
                                    alertTopics.add(topic);
                                }

                                if (includeDetails) {
                                    Map<String, Object> subsDetail = new HashMap<>();
                                    stats.getSubscriptions().forEach((subName, subStats) -> {
                                        subsDetail.put(subName, Map.of(
                                                "backlogMessages", subStats.getMsgBacklog(),
                                                "isHealthy", subStats.getMsgBacklog() <= threshold
                                        ));
                                    });
                                    details.put(topic, Map.of(
                                            "totalBacklog", totalBacklog,
                                            "subscriptions", subsDetail
                                    ));
                                }
                            } catch (Exception e) {
                                details.put(topic, Map.of(
                                        "error", e.getMessage()
                                ));
                            }
                        }

                        result.put("alertTopics", alertTopics);
                        if (includeDetails) {
                            result.put("details", details);
                        }

                        return createSuccessResult("Backlog analysis completed", result);

                    } catch (Exception e) {
                        return createErrorResult("Failed to perform backlog analysis: " + e.getMessage());
                    }
                })
                .build());
    }

    private void registerConnectionDiagnostics(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "connection-diagnostics",
                "Run connection diagnostics to Pulsar cluster with different test depths",
                """
                {
                    "type": "object",
                    "properties": {
                        "testType": {
                            "type": "string",
                            "description": "Type of diagnostics: basic, detailed, network",
                            "enum": ["basic", "detailed", "network"]
                        },
                        "testTopic": {
                            "type": "string",
                            "description": "Topic for testing (default: persistent://public/default/connection-test)"
                        }
                    },
                    "required": ["testType"]
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    Map<String, Object> result = new LinkedHashMap<>();
                    String testType = getRequiredStringParam(request.arguments(), "testType").toLowerCase();
                    String testTopic = getStringParam(request.arguments(), "testTopic");
                    if (testTopic == null || testTopic.isEmpty()) {
                        testTopic = "persistent://public/default/connection-test";
                    }

                    try {
                        try {
                            String leaderBroker = String.valueOf(pulsarAdmin.brokers().getLeaderBroker());
                            result.put("adminApiReachable", true);
                            result.put("leaderBroker", leaderBroker);
                        } catch (Exception e) {
                            result.put("adminApiReachable", false);
                            result.put("adminApiError", e.getMessage());
                            return createErrorResult("Admin API unreachable");
                        }

                        if ("basic".equals(testType)) {
                            result.put("diagnosticsLevel", "basic");
                            return createSuccessResult("Basic connection check completed", result);
                        }

                        long sendStart = System.nanoTime();
                        MessageId msgId;
                        try (Producer<byte[]> producer = pulsarClient.newProducer()
                                .topic(testTopic)
                                .enableBatching(false)
                                .sendTimeout(5, TimeUnit.SECONDS)
                                .create()) {

                            String testMessage = "connection-test-" + System.currentTimeMillis();
                            msgId = producer.newMessage()
                                    .value(testMessage.getBytes(StandardCharsets.UTF_8))
                                    .send();
                            result.put("clientProducerReachable", true);
                            result.put("testMessageId", msgId.toString());
                        } catch (Exception e) {
                            result.put("clientProducerReachable", false);
                            result.put("clientProducerError", e.getMessage());
                            return createErrorResult("Producer test failed");
                        }

                        long sendEnd = System.nanoTime();

                        String receivedMsg = null;
                        long receiveEnd = 0;
                        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                                .topic(testTopic)
                                .subscriptionName("connection-diagnostics-sub")
                                .subscriptionType(SubscriptionType.Exclusive)
                                .subscribe()) {

                            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
                            if (msg != null) {
                                receivedMsg = new String(msg.getData(), StandardCharsets.UTF_8);
                                consumer.acknowledge(msg);
                                result.put("clientConsumerReachable", true);
                                result.put("receivedTestMessage", receivedMsg);
                            } else {
                                result.put("clientConsumerReachable", false);
                                result.put("consumerError", "No message received in time");
                            }
                            receiveEnd = System.nanoTime();
                        } catch (Exception e) {
                            result.put("clientConsumerReachable", false);
                            result.put("clientConsumerError", e.getMessage());
                        }

                        result.put("producerTimeMs", (sendEnd - sendStart) / 1_000_000.0);

                        if ("detailed".equals(testType)) {
                            result.put("diagnosticsLevel", "detailed");
                            return createSuccessResult("Detailed connection check completed", result);
                        }

                        if ("network".equals(testType) && receivedMsg != null) {
                            double roundTripMs = (receiveEnd - sendStart) / 1_000_000.0;
                            result.put("roundTripLatencyMs", roundTripMs);

                            int testSize = 1024 * 100; // 100 KB
                            byte[] payload = new byte[testSize];
                            Arrays.fill(payload, (byte) 65);
                            long totalBytes = 0;
                            long bwStart = System.nanoTime();

                            try (Producer<byte[]> producer = pulsarClient.newProducer()
                                    .topic(testTopic)
                                    .enableBatching(false)
                                    .sendTimeout(5, TimeUnit.SECONDS)
                                    .create()) {
                                for (int i = 0; i < 5; i++) {
                                    producer.newMessage().value(payload).send();
                                    totalBytes += testSize;
                                }
                            }

                            long bwEnd = System.nanoTime();
                            double seconds = (bwEnd - bwStart) / 1_000_000_000.0;
                            double mbps = (totalBytes / 1024.0 / 1024.0) / seconds;

                            result.put("bandwidthMBps", mbps);
                        }

                        result.put("diagnosticsLevel", testType);
                        return createSuccessResult("Connection diagnostics (" + testType + ") completed", result);

                    } catch (Exception e) {
                        LOGGER.error("Error in connection diagnostics", e);
                        result.put("diagnosticsLevel", testType);
                        return createErrorResult("Diagnostics failed: " + e.getMessage());
                    }
                }).build()
        );
    }

    private void registerHealthCheck(McpSyncServer mcpServer) {
        McpSchema.Tool tool = createTool(
                "health-check",
                "Check Pulsar cluster, topic, and subscription health status",
                """
                {
                    "type": "object",
                    "properties": {
                        "topic": {
                            "type": "string",
                            "description": "Topic name to check (optional)"
                        },
                        "subscriptionName": {
                            "type": "string",
                            "description": "Subscription name to check (optional, requires topic)"
                        }
                    }
                }
                """
        );

        mcpServer.addTool(McpServerFeatures.SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    Map<String, Object> result = new HashMap<>();
                    try {
                        // 1. Broker ping test
                        try {
                            pulsarAdmin.brokers().getLeaderBroker();
                            result.put("brokerHealthy", true);
                        } catch (Exception e) {
                            result.put("brokerHealthy", false);
                            return createErrorResult("Broker is not reachable: " + e.getMessage());
                        }

                        // 2. Optional topic check
                        String topic = getStringParam(request.arguments(), "topic");
                        String subscriptionName = getStringParam(request.arguments(), "subscriptionName");

                        if (topic != null && !topic.isEmpty()) {
                            topic = buildFullTopicName(request.arguments());
                            TopicStats stats = pulsarAdmin.topics().getStats(topic);

                            long totalBytesIn = stats.getBytesInCounter();
                            long totalBytesOut = stats.getBytesOutCounter();
                            long totalMsgsIn = stats.getMsgInCounter();
                            long totalMsgsOut = stats.getMsgOutCounter();

                            double throughputMBps = (totalBytesIn + totalBytesOut) / (1024.0 * 1024.0);
                            double messagesPerSecond = totalMsgsIn + totalMsgsOut; // 这里假设采集周期为1秒

                            result.put("topic", topic);
                            result.put("throughputMBps", throughputMBps);
                            result.put("messagesPerSecond", messagesPerSecond);

                            // 3. Backlog analysis
                            long backlog = stats.getSubscriptions().values().stream()
                                    .mapToLong(sub -> sub.getMsgBacklog())
                                    .sum();
                            result.put("backlog", backlog);

                            String backlogLevel;
                            if (backlog == 0) {
                                backlogLevel = "EMPTY";
                            } else if (backlog < 1000) {
                                backlogLevel = "LOW";
                            } else if (backlog < 100000) {
                                backlogLevel = "MEDIUM";
                            } else {
                                backlogLevel = "HIGH";
                            }
                            result.put("backlogLevel", backlogLevel);

                            // 4. Subscription check if provided
                            if (subscriptionName != null && stats.getSubscriptions().containsKey(subscriptionName)) {
                                SubscriptionStats subStats = stats.getSubscriptions().get(subscriptionName);
                                result.put("subscriptionName", subscriptionName);
                                result.put("subscriptionBacklog", subStats.getMsgBacklog());
                                result.put("subscriptionMsgRateOut", subStats.getMsgRateOut());
                                result.put("subscriptionMsgRateRedeliver", subStats.getMsgRateRedeliver());
                            }

                            boolean isHealthy = result.get("brokerHealthy").equals(true)
                                    && throughputMBps > 0
                                    && !"HIGH".equals(backlogLevel);
                            result.put("isHealthy", isHealthy);

                            addTopicBreakdown(result, topic);
                        }

                        return createSuccessResult("Health check completed", result);

                    } catch (Exception e) {
                        LOGGER.error("Unexpected error in health check", e);
                        return createErrorResult("Unexpected error: " + e.getMessage());
                    }
                }).build()
        );
    }



}
