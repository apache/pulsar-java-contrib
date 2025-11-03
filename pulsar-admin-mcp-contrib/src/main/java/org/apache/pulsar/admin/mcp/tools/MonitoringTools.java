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
import org.apache.pulsar.common.policies.data.PublisherStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TopicStats;

public class MonitoringTools extends BasePulsarTools {
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
    McpSchema.Tool tool =
        createTool(
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
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String clusterName = getStringParam(request.arguments(), "clusterName");
                    boolean includeBrokerStats =
                        getBooleanParam(request.arguments(), "includeBrokerStats", true);
                    int maxTopics = getIntParam(request.arguments(), "maxTopics", 10);
                    if (maxTopics < 1) {
                      maxTopics = 1;
                    }
                    if (maxTopics > 50) {
                      maxTopics = 50;
                    }

                    Map<String, Object> result = new HashMap<>();
                    result.put("timestamp", System.currentTimeMillis());

                    var clusters = pulsarAdmin.clusters().getClusters();
                    if (clusterName == null || !clusters.isEmpty()) {
                      clusterName = clusters.get(0);
                    }
                    if (clusterName == null || clusterName.isBlank()) {
                      return createErrorResult("clusterName is required and no clusters found");
                    }
                    result.put("clusterName", clusterName);

                    try {
                      var brokers = pulsarAdmin.brokers().getActiveBrokers(clusterName);
                      result.put("activeBrokers", brokers.size());
                      result.put("brokerList", brokers);

                      if (includeBrokerStats) {
                        Map<String, Object> brokerStats = new HashMap<>();
                        for (String broker : brokers) {
                          try {
                            brokerStats.put(broker, Map.of("status", "active"));
                          } catch (Exception e) {
                            brokerStats.put(
                                broker, Map.of("status", "error", "error", e.getMessage()));
                          }
                        }
                        result.put("brokerStats", brokerStats);
                      }
                    } catch (Exception e) {
                      result.put("brokerError", e.getMessage());
                    }

                    result.put(
                        "clusterHealth", result.containsKey("brokerError") ? "error" : "healthy");
                    return createSuccessResult("Cluster performance monitoring completed", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    return createErrorResult(
                        "Failed to monitor cluster performance:" + e.getMessage());
                  }
                })
            .build());
  }

  private void registerMonitorTopicPerformance(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
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
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String topic = buildFullTopicName(request.arguments());
                    boolean includeDetails =
                        getBooleanParam(request.arguments(), "includeDetails", false);
                    boolean includeInternalStats =
                        getBooleanParam(request.arguments(), "includeInternalStats", false);
                    int maxSubscriptions = getIntParam(request.arguments(), "maxSubscriptions", 10);

                    Map<String, Object> result = new HashMap<>();
                    result.put("timestamp", System.currentTimeMillis());
                    result.put("topic", topic);

                    try {
                      var meta = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);
                      double msgRateIn = 0.0,
                          msgRateOut = 0.0,
                          msgThroughputIn = 0.0,
                          msgThroughputOut = 0.0;
                      long storageSize = 0L;
                      double averageMsgSize = 0.0;
                      int avgCount = 0;

                      Map<String, SubscriptionStats> subMapAgg = new HashMap<>();
                      List<PublisherStats> publishersAgg = new ArrayList<>();

                      if (meta != null && meta.partitions > 0) {
                        for (int i = 0; i < meta.partitions; i++) {
                          var s = pulsarAdmin.topics().getStats(topic + "-partition-" + i);
                          msgRateIn += s.getMsgRateIn();
                          msgRateOut += s.getMsgRateOut();
                          msgThroughputIn += s.getMsgThroughputIn();
                          msgThroughputOut += s.getMsgThroughputOut();
                          storageSize += s.getStorageSize();
                          if (s.getAverageMsgSize() > 0) {
                            averageMsgSize += s.getAverageMsgSize();
                            avgCount++;
                          }

                          if (s.getSubscriptions() != null) {
                            s.getSubscriptions()
                                .forEach(
                                    (k, v) ->
                                        subMapAgg.merge(
                                            k,
                                            v,
                                            (a, b) -> {
                                              return b;
                                            }));
                          }
                          if (s.getPublishers() != null) {
                            publishersAgg.addAll(s.getPublishers());
                          }
                        }
                      } else {
                        var s = pulsarAdmin.topics().getStats(topic);
                        msgRateIn = s.getMsgRateIn();
                        msgRateOut = s.getMsgRateOut();
                        msgThroughputIn = s.getMsgThroughputIn();
                        msgThroughputOut = s.getMsgThroughputOut();
                        storageSize = s.getStorageSize();
                        averageMsgSize = s.getAverageMsgSize();
                        avgCount = (averageMsgSize > 0) ? 1 : 0;
                        if (s.getSubscriptions() != null) {
                          subMapAgg.putAll(s.getSubscriptions());
                        }
                        if (s.getPublishers() != null) {
                          publishersAgg.addAll(s.getPublishers());
                        }
                      }

                      result.put("msgRateIn", msgRateIn);
                      result.put("msgRateOut", msgRateOut);
                      result.put("msgThroughputIn", msgThroughputIn);
                      result.put("msgThroughputOut", msgThroughputOut);
                      result.put("storageSize", storageSize);
                      result.put(
                          "averageMsgSize", avgCount == 0 ? 0.0 : (averageMsgSize / avgCount));

                      int publishersCount = publishersAgg == null ? 0 : publishersAgg.size();
                      int subscriptionsCount = subMapAgg == null ? 0 : subMapAgg.size();
                      result.put("publishersCount", publishersCount);
                      result.put("subscriptionsCount", subscriptionsCount);

                      int totalConsumers = 0;
                      long totalBacklog = 0L;
                      if (subMapAgg != null) {
                        for (var sub : subMapAgg.values()) {
                          if (sub.getConsumers() != null) {
                            totalConsumers += sub.getConsumers().size();
                          }
                          totalBacklog += sub.getMsgBacklog();
                        }
                      }
                      result.put("totalConsumers", totalConsumers);
                      result.put("totalBacklog", totalBacklog);

                      if (includeDetails && subMapAgg != null) {
                        Map<String, Object> subscriptionStats = new HashMap<>();
                        List<String> subscriptionNames = new ArrayList<>(subMapAgg.keySet());
                        if (subscriptionNames.size() > maxSubscriptions) {
                          subscriptionNames = subscriptionNames.subList(0, maxSubscriptions);
                        }
                        for (String subName : subscriptionNames) {
                          var sub = subMapAgg.get(subName);
                          Map<String, Object> subDetail = new HashMap<>();
                          subDetail.put("msgRateOut", sub.getMsgRateOut());
                          subDetail.put("msgThroughputOut", sub.getMsgThroughputOut());
                          subDetail.put("msgBacklog", sub.getMsgBacklog());
                          subDetail.put("msgRateExpired", sub.getMsgRateExpired());
                          subDetail.put(
                              "consumersCount",
                              sub.getConsumers() == null ? 0 : sub.getConsumers().size());
                          subDetail.put("type", sub.getType());

                          List<Map<String, Object>> consumerDetails = new ArrayList<>();
                          if (sub.getConsumers() != null) {
                            for (var consumer : sub.getConsumers()) {
                              Map<String, Object> consumerInfo = new HashMap<>();
                              consumerInfo.put("consumerName", consumer.getConsumerName());
                              consumerInfo.put("msgRateOut", consumer.getMsgRateOut());
                              consumerInfo.put("msgThroughputOut", consumer.getMsgThroughputOut());
                              consumerInfo.put("availablePermits", consumer.getAvailablePermits());
                              consumerInfo.put("unackedMessages", consumer.getUnackedMessages());
                              consumerDetails.add(consumerInfo);
                            }
                          }
                          subDetail.put("consumers", consumerDetails);
                          subscriptionStats.put(subName, subDetail);
                        }
                        result.put("subscriptionStats", subscriptionStats);

                        List<Map<String, Object>> publisherDetails = new ArrayList<>();
                        if (publishersAgg != null) {
                          for (var publisher : publishersAgg) {
                            Map<String, Object> pubInfo = new HashMap<>();
                            pubInfo.put("producerName", publisher.getProducerName());
                            pubInfo.put("msgRateIn", publisher.getMsgRateIn());
                            pubInfo.put("msgThroughputIn", publisher.getMsgThroughputIn());
                            pubInfo.put("averageMsgSize", publisher.getAverageMsgSize());
                            publisherDetails.add(pubInfo);
                          }
                        }
                        result.put("publisherDetails", publisherDetails);
                      }

                      if (includeInternalStats) {
                        try {
                          var internalStats =
                              (meta != null && meta.partitions > 0)
                                  ? pulsarAdmin.topics().getInternalStats(topic + "-partition-0")
                                  : pulsarAdmin.topics().getInternalStats(topic);
                          Map<String, Object> internal = new HashMap<>();
                          internal.put("numberOfEntries", internalStats.numberOfEntries);
                          internal.put("totalSize", internalStats.totalSize);
                          internal.put("currentLedgerEntries", internalStats.currentLedgerEntries);
                          internal.put("currentLedgerSize", internalStats.currentLedgerSize);
                          internal.put(
                              "ledgerCount",
                              internalStats.ledgers == null ? 0 : internalStats.ledgers.size());
                          internal.put(
                              "cursorCount",
                              internalStats.cursors == null ? 0 : internalStats.cursors.size());
                          result.put("internalStats", internal);
                        } catch (Exception e) {
                          result.put("internalStatsError", e.getMessage());
                        }
                      }

                    } catch (Exception e) {
                      result.put("statsError", e.getMessage());
                    }

                    String topicHealth = "healthy";
                    if (result.containsKey("statsError")) {
                      topicHealth = "error";
                    } else {
                      Long totalBacklogVal = (Long) result.get("totalBacklog");
                      Double msgRateInVal = (Double) result.get("msgRateIn");
                      Double msgRateOutVal = (Double) result.get("msgRateOut");
                      if (totalBacklogVal != null && totalBacklogVal > 100_000) {
                        topicHealth = "backlog_high";
                      } else if (msgRateInVal != null
                          && msgRateOutVal != null
                          && msgRateInVal > msgRateOutVal * 1.5) {
                        topicHealth = "consumption_slow";
                      }
                    }
                    result.put("topicHealth", topicHealth);

                    addTopicBreakdown(result, topic);
                    return createSuccessResult("Topic performance monitoring completed", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    return createErrorResult(
                        "Failed to monitor topic performance: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerMonitorSubscriptionPerformance(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
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
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String topic = buildFullTopicName(request.arguments());
                    String subscriptionName =
                        getRequiredStringParam(request.arguments(), "subscriptionName");

                    var meta = pulsarAdmin.topics().getPartitionedTopicMetadata(topic);

                    long msgBacklog = 0L;
                    double msgRateOut = 0.0, msgThroughputOut = 0.0;
                    int consumersCount = 0;
                    boolean blockedOnUnacked = false;
                    Object subscriptionType = null;

                    if (meta != null && meta.partitions > 0) {
                      for (int i = 0; i < meta.partitions; i++) {
                        var stats = pulsarAdmin.topics().getStats(topic + "-partition-" + i);
                        var sub =
                            (stats.getSubscriptions() == null)
                                ? null
                                : stats.getSubscriptions().get(subscriptionName);
                        if (sub == null) {
                          continue;
                        }

                        msgBacklog += sub.getMsgBacklog();
                        msgRateOut += sub.getMsgRateOut();
                        msgThroughputOut += sub.getMsgThroughputOut();
                        consumersCount +=
                            (sub.getConsumers() == null ? 0 : sub.getConsumers().size());
                        blockedOnUnacked =
                            blockedOnUnacked || sub.isBlockedSubscriptionOnUnackedMsgs();
                        if (subscriptionType == null) {
                          subscriptionType = sub.getType();
                        }
                      }
                      if (msgBacklog == 0 && consumersCount == 0 && subscriptionType == null) {
                        return createErrorResult("Subscription not found: " + subscriptionName);
                      }
                    } else {
                      TopicStats stats = pulsarAdmin.topics().getStats(topic);
                      var subscriptions = stats.getSubscriptions();
                      if (subscriptions == null || !subscriptions.containsKey(subscriptionName)) {
                        return createErrorResult("Subscription not found: " + subscriptionName);
                      }
                      var sub = subscriptions.get(subscriptionName);
                      msgBacklog = sub.getMsgBacklog();
                      msgRateOut = sub.getMsgRateOut();
                      msgThroughputOut = sub.getMsgThroughputOut();
                      consumersCount = (sub.getConsumers() == null ? 0 : sub.getConsumers().size());
                      blockedOnUnacked = sub.isBlockedSubscriptionOnUnackedMsgs();
                      subscriptionType = sub.getType();
                    }

                    Map<String, Object> result = new HashMap<>();
                    result.put("topic", topic);
                    result.put("subscriptionName", subscriptionName);
                    result.put("timestamp", System.currentTimeMillis());
                    result.put("msgBacklog", msgBacklog);
                    result.put("msgRateOut", msgRateOut);
                    result.put("msgThroughputOut", msgThroughputOut);
                    result.put("consumersCount", consumersCount);
                    result.put("blockedSubscriptionOnUnackedMsgs", blockedOnUnacked);
                    result.put("subscriptionType", subscriptionType);

                    addTopicBreakdown(result, topic);
                    return createSuccessResult("Subscription performance retrieved", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Failed to monitor subscription performance", e);
                    return createErrorResult(
                        "Failed to monitor subscription performance: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerBacklogAnalysis(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
            "backlog-analysis",
            "Analyze message backlog within a Pulsar namespace "
                + "and report topics/subscriptions exceeding a given threshold",
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
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  try {
                    String namespace = getStringParam(request.arguments(), "namespace");
                    int threshold = getIntParam(request.arguments(), "threshold", 1000);
                    boolean includeDetails =
                        getBooleanParam(request.arguments(), "includeDetails", false);

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
                        long totalBacklog =
                            stats.getSubscriptions().values().stream()
                                .mapToLong(sub -> sub.getMsgBacklog())
                                .sum();

                        if (totalBacklog > threshold) {
                          alertTopics.add(topic);
                        }

                        if (includeDetails) {
                          Map<String, Object> subsDetail = new HashMap<>();
                          stats
                              .getSubscriptions()
                              .forEach(
                                  (subName, subStats) -> {
                                    subsDetail.put(
                                        subName,
                                        Map.of(
                                            "backlogMessages",
                                            subStats.getMsgBacklog(),
                                            "isHealthy",
                                            subStats.getMsgBacklog() <= threshold));
                                  });
                          details.put(
                              topic,
                              Map.of(
                                  "totalBacklog", totalBacklog,
                                  "subscriptions", subsDetail));
                        }
                      } catch (Exception e) {
                        details.put(topic, Map.of("error", e.getMessage()));
                      }
                    }

                    result.put("alertTopics", alertTopics);
                    if (includeDetails) {
                      result.put("details", details);
                    }

                    return createSuccessResult("Backlog analysis completed", result);

                  } catch (Exception e) {
                    return createErrorResult(
                        "Failed to perform backlog analysis: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerConnectionDiagnostics(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
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
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  Map<String, Object> result = new LinkedHashMap<>();
                  String testType =
                      getRequiredStringParam(request.arguments(), "testType").toLowerCase();
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
                    String subName = "connection-diagnostics-sub-" + System.currentTimeMillis();
                    String sentPayload = "connection-test-" + System.currentTimeMillis();

                    long sendStartNs;
                    long sendEndNs;
                    long receiveEndNs = 0L;

                    try (Consumer<byte[]> consumer =
                            pulsarClient
                                .newConsumer()
                                .topic(testTopic)
                                .subscriptionName(subName)
                                .subscriptionType(SubscriptionType.Exclusive)
                                .subscribe();
                        Producer<byte[]> producer =
                            pulsarClient
                                .newProducer()
                                .topic(testTopic)
                                .enableBatching(false)
                                .sendTimeout(5, TimeUnit.SECONDS)
                                .create()) {

                      sendStartNs = System.nanoTime();
                      MessageId msgId =
                          producer
                              .newMessage()
                              .value(sentPayload.getBytes(StandardCharsets.UTF_8))
                              .send();
                      sendEndNs = System.nanoTime();
                      result.put("clientProducerReachable", true);
                      result.put("testMessageId", msgId.toString());

                      Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
                      if (msg != null) {
                        String received = new String(msg.getData(), StandardCharsets.UTF_8);
                        if (sentPayload.equals(received)) {
                          result.put("clientConsumerReachable", true);
                          result.put("receivedTestMessage", received);
                        } else {
                          result.put("clientConsumerReachable", false);
                          result.put("consumerError", "Received unexpected payload");
                        }
                        consumer.acknowledge(msg);
                        receiveEndNs = System.nanoTime();
                      } else {
                        result.put("clientConsumerReachable", false);
                        result.put("consumerError", "No message received in time");
                      }
                    } catch (Exception e) {
                      result.put("clientProducerReachable", false);
                      result.put("clientProducerError", e.getMessage());
                      return createErrorResult("Producer/Consumer test failed");
                    }

                    result.put("producerTimeMs", (sendEndNs - sendStartNs) / 1_000_000.0);

                    if ("detailed".equals(testType)) {
                      result.put("diagnosticsLevel", "detailed");
                      return createSuccessResult("Detailed connection check completed", result);
                    }

                    if ("network".equals(testType)
                        && Boolean.TRUE.equals(result.get("clientConsumerReachable"))) {
                      double roundTripMs = (receiveEndNs - sendStartNs) / 1_000_000.0;
                      result.put("roundTripLatencyMs", roundTripMs);

                      int testSize = 1024 * 100;
                      byte[] payload = new byte[testSize];
                      Arrays.fill(payload, (byte) 65);
                      long totalBytes = 0;
                      long bwStart = System.nanoTime();

                      try (Producer<byte[]> producer =
                          pulsarClient
                              .newProducer()
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
                    return createSuccessResult(
                        "Connection diagnostics (" + testType + ") completed", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Error in connection diagnostics", e);
                    result.put("diagnosticsLevel", testType);
                    return createErrorResult("Diagnostics failed: " + e.getMessage());
                  }
                })
            .build());
  }

  private void registerHealthCheck(McpSyncServer mcpServer) {
    McpSchema.Tool tool =
        createTool(
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
                """);

    mcpServer.addTool(
        McpServerFeatures.SyncToolSpecification.builder()
            .tool(tool)
            .callHandler(
                (exchange, request) -> {
                  Map<String, Object> result = new HashMap<>();
                  try {
                    try {
                      pulsarAdmin.brokers().getLeaderBroker();
                      result.put("brokerHealthy", true);
                    } catch (Exception e) {
                      result.put("brokerHealthy", false);
                      return createErrorResult("Broker is not reachable: " + e.getMessage());
                    }

                    String topic = getStringParam(request.arguments(), "topic");
                    String subscriptionName =
                        getStringParam(request.arguments(), "subscriptionName");

                    if (topic != null && !topic.isEmpty()) {
                      topic = buildFullTopicName(request.arguments());
                      TopicStats stats = pulsarAdmin.topics().getStats(topic);

                      double throughputMBps =
                          (stats.getMsgThroughputIn() + stats.getMsgThroughputOut())
                              / (1024.0 * 1024.0);
                      double messagesPerSecond = (stats.getMsgRateIn() + stats.getMsgRateOut());

                      result.put("topic", topic);
                      result.put("throughputMBps", throughputMBps);
                      result.put("messagesPerSecond", messagesPerSecond);

                      long backlog =
                          stats.getSubscriptions().values().stream()
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

                      if (subscriptionName != null
                          && stats.getSubscriptions().containsKey(subscriptionName)) {
                        SubscriptionStats subStats = stats.getSubscriptions().get(subscriptionName);
                        result.put("subscriptionName", subscriptionName);
                        result.put("subscriptionBacklog", subStats.getMsgBacklog());
                        result.put("subscriptionMsgRateOut", subStats.getMsgRateOut());
                        result.put("subscriptionMsgRateRedeliver", subStats.getMsgRateRedeliver());
                      }

                      boolean isHealthy =
                          result.get("brokerHealthy").equals(true)
                              && throughputMBps > 0
                              && !"HIGH".equals(backlogLevel);
                      result.put("isHealthy", isHealthy);

                      addTopicBreakdown(result, topic);
                    }

                    return createSuccessResult("Health check completed", result);

                  } catch (IllegalArgumentException e) {
                    return createErrorResult(e.getMessage());
                  } catch (Exception e) {
                    LOGGER.error("Unexpected error in health check", e);
                    return createErrorResult("Unexpected error: " + e.getMessage());
                  }
                })
            .build());
  }
}
