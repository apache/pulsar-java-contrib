# Pulsar Admin MCP Contrib

基于 Model Context Protocol (MCP) 的 Apache Pulsar 管理服务端，支持 AI 助手通过统一接口管理 Pulsar 集群（支持 HTTP Streaming 和 STDIO 两种传输模式）。

## 快速开始

### 依赖

- Java 17+
- Maven 3.6+
- Pulsar 2.10+（3.x 优先）
- MCP Java SDK 0.12.0
- Jetty 11.0.20

## 0. 启动 Pulsar

### 方式 A: Docker
```bash
docker run -it --name pulsar -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:3.2.4 bin/pulsar standalone
```

- **Service URL**: `pulsar://localhost:6650`
- **Admin URL**: `http://localhost:8080`

### 方式 B: 本地二进制
```bash
bin/pulsar standalone
```

## 1. 编译

```bash
mvn clean package -DskipTests
```

**输出**：`target/mcp-contrib-1.0.0-SNAPSHOT.jar`

## 2. 启动 MCP Server

### HTTP 模式（推荐：Web/远程）
```bash
java -jar target/mcp-contrib-1.0.0-SNAPSHOT.jar --transport http --port 8889
```

**日志示例**：
```
HTTP Streamable transport ready at http://localhost:8889/mcp/stream
```

**健康检查**：
```bash
curl -i http://localhost:8889/mcp/stream
```

### STDIO 模式（推荐：Claude Desktop / 本地 IDE）
```bash
java -jar target/mcp-contrib-1.0.0-SNAPSHOT.jar --transport stdio
```

## 3. 客户端配置

### Claude Desktop

#### Windows 配置

**配置文件位置**：`%APPDATA%\Claude\claude_desktop_config.json`

**配置步骤**：
1. 打开 Claude Desktop 配置文件
2. 添加以下配置到 `mcpServers` 部分：

```json
{
  "mcpServers": {
    "pulsar-admin": {
      "command": "java",
      "args": ["-jar", "编译后的包所在的目录，例如 E:\\projects\\pulsar-admin-mcp-contrib\\target\\mcp-contrib-1.0.0-SNAPSHOT.jar", "--transport", "stdio"],
      "cwd": "项目所在目录，例如E:\\projects\\pulsar-admin-mcp-contrib",
      "env": {
        "PULSAR_SERVICE_URL": "pulsar://localhost:6650",
        "PULSAR_ADMIN_URL": "http://localhost:8080"
      }
    }
  }
}
```

#### macOS 配置

**配置文件位置**：`~/Library/Application Support/Claude/claude_desktop_config.json`

**配置步骤**：
1. 打开 Claude Desktop 配置文件
2. 添加以下配置到 `mcpServers` 部分：

```json
{
  "mcpServers": {
    "pulsar-admin": {
      "command": "java",
      "args": ["-jar", "编译后的包所在的目录，例如 /Users/username/projects/pulsar-admin-mcp-contrib/target/mcp-contrib-1.0.0-SNAPSHOT.jar", "--transport", "stdio"],
      "cwd": "项目所在目录，例如/Users/username/projects/pulsar-admin-mcp-contrib",
      "env": {
        "PULSAR_SERVICE_URL": "pulsar://localhost:6650",
        "PULSAR_ADMIN_URL": "http://localhost:8080"
      }
    }
  }
}
```

**注意事项**：
- 请将 `编译后的包所在的目录` 替换为实际的 JAR 文件路径
- 请将 `项目所在目录` 替换为实际的项目根目录路径
- 确保 Java 环境变量已正确配置
- Windows 使用反斜杠 `\`，macOS 使用正斜杠 `/`

### Cherry Studio

#### STDIO 模式配置
同上

#### HTTP 模式配置

**配置步骤**：
1. 确保 MCP 服务器已启动（HTTP 模式）
2. 在 Cherry Studio 中添加 HTTP 类型配置：

```json
{
  "mcpServers": {
    "pulsar-admin-http": {
      "type": "http",
      "url": "http://localhost:8889/mcp/stream",
      "env": {
        "PULSAR_SERVICE_URL": "pulsar://localhost:6650",
        "PULSAR_ADMIN_URL": "http://localhost:8080"
      }
    }
  }
}
```

**配置说明**：
- **STDIO 模式**：适合本地开发，需要指定完整的 JAR 文件路径
- **HTTP 模式**：适合远程访问，需要先启动 HTTP 服务器
- 两种模式的环境变量配置相同，用于指定 Pulsar 集群连接信息
- Windows 使用反斜杠 `\`，macOS 使用正斜杠 `/`

## 4. 配置项

### 环境变量
- `PULSAR_SERVICE_URL`（默认 `pulsar://localhost:6650`）
- `PULSAR_ADMIN_URL`（默认 `http://localhost:8080`）

### 命令行参数
- `--transport`：http / stdio
- `--port`：HTTP 端口（默认 8889）

## 5. 工具清单

覆盖 **集群** / **租户** / **命名空间** / **主题** / **订阅** / **消息** / **Schema** / **监控** 8 大类，共 71 个工具：

### 集群管理（10 个工具）
- `list-clusters` - 列出所有 Pulsar 集群及其状态
- `get-cluster-info` - 获取特定集群的详细信息
- `create-cluster` - 创建新的 Pulsar 集群
- `update-cluster-config` - 更新现有集群的配置
- `delete-cluster` - 按名称删除 Pulsar 集群
- `get-cluster-stats` - 获取指定集群的统计信息
- `list-brokers` - 列出集群中的所有活跃代理
- `get-broker-stats` - 获取特定代理的统计信息
- `get-cluster-failure-domain` - 获取集群的故障域
- `set-cluster-failure-domain` - 设置或更新故障域配置

### 租户管理（6 个工具）
- `list-tenants` - 列出所有 Pulsar 租户
- `get-tenant-info` - 获取特定租户的信息
- `create-tenant` - 创建新的 Pulsar 租户
- `update-tenant` - 更新租户配置
- `delete-tenant` - 删除特定租户
- `get-tenant-stats` - 获取租户的统计信息

### 命名空间管理（10 个工具）
- `list-namespaces` - 列出所有命名空间
- `get-namespace-info` - 获取命名空间信息
- `create-namespace` - 创建新的命名空间
- `delete-namespace` - 删除命名空间
- `set-retention-policy` - 为命名空间设置保留策略
- `get-retention-policy` - 获取命名空间的保留策略
- `set-backlog-quota` - 为命名空间设置积压配额
- `get-backlog-quota` - 获取命名空间的积压配额
- `clear-namespace-backlog` - 清除命名空间的积压
- `get-namespace-stats` - 获取命名空间统计信息

### 主题管理（15 个工具）
- `list-topics` - 列出所有主题
- `create-topic` - 创建新主题
- `delete-topic` - 删除主题
- `get-topic-stats` - 获取主题统计信息
- `get-topic-metadata` - 获取主题元数据
- `update-topic-partitions` - 更新主题分区数量
- `compact-topic` - 压缩主题
- `unload-topic` - 卸载主题
- `get-topic-backlog` - 获取主题积压信息
- `expire-topic-messages` - 使主题中的消息过期
- `peek-messages` - 从主题中查看消息
- `peek-topic-messages` - 从主题中查看消息而不消费
- `reset-topic-cursor` - 重置主题游标
- `get-topic-internal-stats` - 获取主题内部统计信息
- `get-partitioned-metadata` - 获取分区主题元数据

### 订阅管理（10 个工具）
- `list-subscriptions` - 列出所有订阅
- `create-subscription` - 创建新订阅
- `delete-subscription` - 删除订阅
- `get-subscription-stats` - 获取订阅统计信息
- `reset-subscription-cursor` - 重置订阅游标
- `skip-messages` - 跳过订阅中的消息
- `expire-subscription-messages` - 使订阅中的消息过期
- `pause-subscription` - 暂停订阅
- `resume-subscription` - 恢复订阅
- `unsubscribe` - 取消订阅主题

### 消息操作（8 个工具）
- `peek-message` - 从订阅中查看消息而不确认
- `examine-messages` - 检查主题中的消息而不消费
- `skip-all-messages` - 跳过订阅中的所有消息
- `expire-all-messages` - 使订阅中的所有消息过期
- `get-message-backlog` - 获取订阅的消息积压数量
- `send-message` - 向主题发送消息
- `get-message-stats` - 获取主题或订阅的消息统计信息
- `receive-messages` - 从主题接收消息

### Schema 管理（6 个工具）
- `get-schema-info` - 获取主题的 Schema 信息
- `get-schema-version` - 获取主题的 Schema 版本
- `get-all-schema-versions` - 获取主题的所有 Schema 版本
- `upload-schema` - 向主题上传新的 Schema
- `delete-schema` - 删除主题的 Schema
- `test-schema-compatibility` - 测试 Schema 兼容性

### 监控与诊断（6 个工具）
- `monitor-cluster-performance` - 监控集群性能指标
- `monitor-topic-performance` - 监控主题性能指标
- `monitor-subscription-performance` - 监控订阅性能
- `health-check` - 检查集群、主题和订阅的健康状态
- `connection-diagnostics` - 运行不同测试深度的连接诊断
- `backlog-analysis` - 分析命名空间内的消息积压

> **说明**：仅初始化 PulsarAdmin 时，消息发送/消费相关会返回 `not_implemented`；要启用需初始化 PulsarClient 并创建 producer/consumer。

## 自然语言交互 Demo（Use Cases）

下列示例展示在 MCP 客户端里，用自然语言触发工具调用的典型流程。实际返回字段随集群而异。

### 1. 租户与命名空间管理

**Prompt：**
> 帮我看看集群里有哪些租户；在 tenant1 下创建命名空间 ns-orders，然后把这个命名空间的统计给我看看。

**触发：**
`list-tenants` → `create-namespace(tenant=tenant1, namespace=ns-orders)` → `get-namespace-stats(...)`

### 2. 创建分区主题并扩容

**Prompt：**
> 在 public/default 下建个主题 orders，分区数 8；然后把分区数扩到 16，给我分区元数据。

**触发：**
`create-topic(partitions=8)` → `update-topic-partitions(16)` → `get-partitioned-metadata`

### 3. 清理积压并做 compaction

**Prompt：**
> public/default/orders backlog 很大，清一下；再做一次 compaction。

**触发：**
`get-topic-backlog` → `expire-topic-messages/clear-namespace-backlog` → `compact-topic`

### 4. Schema 上载与兼容性测试

**Prompt：**
> 给 persistent://public/default/orders 设置 Avro schema：orderId:string, amount:double，并检查兼容性。

**触发：**
`upload-schema(type=AVRO, schemaJson=...)` → `test-schema-compatibility` / `get-schema-info`

### 5. 订阅管理与游标重置

**Prompt：**
> 在 orders 上创建 failover 订阅 sub-a，再把游标回拨到 1 小时前。

**触发：**
`create-subscription(type=failover)` → `reset-subscription-cursor(timestamp=now-1h)`

## 最佳实践

- **Topic 命名**：完整名形如 `persistent://tenant/namespace/topic`。允许短名输入，服务端会规范化。

- **失败域**：为 Broker/Bookie 设置 Failure Domain，提升机架/可用区级容灾。

## 故障排查

### NoClassDefFoundError: LogarithmicArrayByteBufferPool
Jetty 版本混用。统一使用 Jetty 11：
```xml
<dependency>
    <groupId>org.eclipse.jetty</groupId>
    <artifactId>jetty-server</artifactId>
    <version>11.0.20</version>
</dependency>
<dependency>
    <groupId>org.eclipse.jetty</groupId>
    <artifactId>jetty-ee9-servlet</artifactId>
    <version>11.0.20</version>
</dependency>
```
并使用 `org.eclipse.jetty.ee9.servlet.*` 的 import。

### STDIO 模式 JSON 被日志污染
关闭/降低 stdout 日志，把错误输出到 stderr；stdout 只输出 MCP JSON。

### 消息发送/接收不可用
未初始化 PulsarClient，或 producer/consumer 未创建。仅 Admin 可用时会返回 `not_implemented`。

### Expire message … due to ongoing message expiration
同一分区已有过期任务在跑；等待完成或对分区逐个执行；结合 `get-topic-internal-stats` 观察。

## 许可证

Apache License 2.0（详见 [LICENSE](../../LICENSE)）。
