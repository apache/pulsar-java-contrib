# Pulsar Admin MCP Contrib

Apache Pulsar administration server based on Model Context Protocol (MCP), enabling AI assistants to manage Pulsar clusters through a unified interface (supports both HTTP streaming and STDIO transports).

## Key Features

- **Dual Transport**: HTTP Stream (`/mcp/stream`) and STDIO
- **Full Stack Management**: Cluster / Tenant / Namespace / Topic / Subscription / (Optional) Message
- **Monitoring Tools**: Health checks, performance metrics, backlog analysis
- **Schema Management**: Upload / Query / Compatibility testing
- **Authentication Support**: Plugin-based (e.g., Token)
- **Tool Whitelist/Blacklist**: Enable/disable tools by name

## Quick Start

### Prerequisites

- Java 17+
- Maven 3.6+
- Accessible Apache Pulsar (local or remote)

### Building

```bash
mvn clean package -DskipTests
```

### Running

#### HTTP (Recommended for Web / Remote)

```bash
java -jar target/mcp-contrib-1.0.0-SNAPSHOT.jar --transport http --port 8889
# Success log example:
# HTTP Streamable transport ready at http://localhost:8889/mcp/stream
```

**Health Check:**

```bash
curl -i http://localhost:8889/mcp/stream
```

Returns 200/405/404 are all acceptable, indicating the endpoint is responding (whether 200 depends on request method/body).

#### STDIO (Recommended for Claude Desktop and other local integrations)

```bash
java -jar target/mcp-contrib-1.0.0-SNAPSHOT.jar --transport stdio
```

**Claude Desktop Configuration Example:**

```json
{
  "mcpServers": {
    "pulsar-admin": {
      "command": "java",
      "args": [
        "-jar",
        "E:\\projects2\\pulsar-admin-mcp-contrib\\target\\mcp-contrib-1.0.0-SNAPSHOT.jar",
        "--transport", "stdio"
      ],
      "cwd": "E:\\projects2\\pulsar-admin-mcp-contrib",
      "env": {
        "PULSAR_SERVICE_URL": "pulsar://localhost:6650",
        "PULSAR_ADMIN_URL": "http://localhost:8080"
      }
    }
  }
}
```

> **Note**: The environment variable name is `PULSAR_SERVICE_URL` (not `PULSAR_SERVER_URL`).

## Configuration

### Environment Variables (Priority: Command Line > Environment Variables > Default)

- `PULSAR_SERVICE_URL` (default: `pulsar://localhost:6650`)
- `PULSAR_ADMIN_URL` (default: `http://localhost:8080`)

### Command Line Arguments

- `--transport`: `http` / `stdio` / `all`
- `--port`: HTTP port (default: 8889)
- `--service-url` / `--admin-url`: Override connection addresses
- `--auth-plugin` / `--auth-params`: Authentication (e.g., Token)
- `--allow-tools` / `--block-tools`: Whitelist/blacklist tools by name (comma-separated)

### Authentication Example (Token)

```bash
java \
 -Dpulsar.auth.plugin=org.apache.pulsar.client.impl.auth.AuthenticationToken \
 -Dpulsar.auth.params=token:xxxx \
 -jar target/mcp-contrib-1.0.0-SNAPSHOT.jar --transport http
```

### Configuration File (Optional)

**config.properties**

```properties
pulsar.serviceUrl=pulsar://localhost:6650
pulsar.admin.url=http://localhost:8080
mcp.transport=http
mcp.http.port=8889
mcp.auth.plugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
mcp.auth.params=token:your-token-here
```

## Tool Inventory (Complete List)

### Cluster Management (10 tools)
- `list-clusters` - List all Pulsar clusters and their status
- `get-cluster-info` - Get detailed information about a specific cluster
- `create-cluster` - Create a new Pulsar cluster
- `update-cluster-config` - Update configuration of an existing cluster
- `delete-cluster` - Delete a Pulsar cluster by name
- `get-cluster-stats` - Get statistics for a given cluster
- `list-brokers` - List all active brokers in a cluster
- `get-broker-stats` - Get statistics for a specific broker
- `get-cluster-failure-domain` - Get failure domains for a cluster
- `set-cluster-failure-domain` - Set or update failure domain configuration

### Tenant Management (6 tools)
- `list-tenants` - List all Pulsar tenants
- `get-tenant-info` - Get information about a specific tenant
- `create-tenant` - Create a new Pulsar tenant
- `update-tenant` - Update tenant configuration
- `delete-tenant` - Delete a specific tenant
- `get-tenant-stats` - Get statistics for a tenant

### Namespace Management (10 tools)
- `list-namespaces` - List all namespaces
- `get-namespace-info` - Get namespace information
- `create-namespace` - Create a new namespace
- `delete-namespace` - Delete a namespace
- `set-retention-policy` - Set retention policy for a namespace
- `get-retention-policy` - Get retention policy for a namespace
- `set-backlog-quota` - Set backlog quota for a namespace
- `get-backlog-quota` - Get backlog quota for a namespace
- `clear-namespace-backlog` - Clear backlog for a namespace
- `get-namespace-stats` - Get namespace statistics

### Topic Management (15 tools)
- `list-topics` - List all topics
- `create-topic` - Create a new topic
- `delete-topic` - Delete a topic
- `get-topic-stats` - Get topic statistics
- `get-topic-metadata` - Get topic metadata
- `update-topic-partitions` - Update topic partition count
- `compact-topic` - Compact a topic
- `unload-topic` - Unload a topic
- `get-topic-backlog` - Get topic backlog information
- `expire-topic-messages` - Expire messages in a topic
- `peek-messages` - Peek messages from a topic
- `peek-topic-messages` - Peek messages from a topic without consuming
- `reset-topic-cursor` - Reset topic cursor
- `get-topic-internal-stats` - Get internal topic statistics
- `get-partitioned-metadata` - Get partitioned topic metadata

### Subscription Management (10 tools)
- `list-subscriptions` - List all subscriptions
- `create-subscription` - Create a new subscription
- `delete-subscription` - Delete a subscription
- `get-subscription-stats` - Get subscription statistics
- `reset-subscription-cursor` - Reset subscription cursor
- `skip-messages` - Skip messages in a subscription
- `expire-subscription-messages` - Expire messages in a subscription
- `pause-subscription` - Pause a subscription
- `resume-subscription` - Resume a subscription
- `unsubscribe` - Unsubscribe from a topic

### Message Operations (8 tools)
- `peek-message` - Peek messages from a subscription without acknowledging
- `examine-messages` - Examine messages from a topic without consuming
- `skip-all-messages` - Skip all messages in a subscription
- `expire-all-messages` - Expire all messages in a subscription
- `get-message-backlog` - Get message backlog count for a subscription
- `send-message` - Send a message to a topic
- `get-message-stats` - Get message statistics for a topic or subscription
- `receive-messages` - Receive messages from a topic

### Schema Management (6 tools)
- `get-schema-info` - Get schema information for a topic
- `get-schema-version` - Get schema version for a topic
- `get-all-schema-versions` - Get all schema versions for a topic
- `upload-schema` - Upload a new schema to a topic
- `delete-schema` - Delete schema for a topic
- `test-schema-compatibility` - Test schema compatibility

### Monitoring & Diagnostics (6 tools)
- `monitor-cluster-performance` - Monitor cluster performance metrics
- `monitor-topic-performance` - Monitor topic performance metrics
- `monitor-subscription-performance` - Monitor subscription performance
- `health-check` - Check cluster, topic, and subscription health
- `connection-diagnostics` - Run connection diagnostics with different test depths
- `backlog-analysis` - Analyze message backlog within a namespace

### Tool Summary
**Total: 69 tools** across 7 categories:
- Cluster Management: 10 tools
- Tenant Management: 6 tools
- Namespace Management: 10 tools
- Topic Management: 15 tools
- Subscription Management: 10 tools
- Message Operations: 8 tools
- Schema Management: 6 tools
- Monitoring & Diagnostics: 6 tools

> **Note**: When only PulsarAdmin is initialized, message send/consume related operations will return `not_implemented`; to enable them, PulsarClient needs to be initialized and producer/consumer created.

## Natural Language Interaction Demo (Use Cases)

The following examples demonstrate typical workflows of triggering tool calls with natural language in MCP clients. Actual returned fields vary by cluster.

### 1. Tenant and Namespace Management

**Prompt:**
> Show me what tenants are in the cluster; create namespace ns-orders under tenant1, then show me the statistics for this namespace.

**Possible triggers:**
`list-tenants` → `create-namespace(tenant=tenant1, namespace=ns-orders)` → `get-namespace-stats(...)`

### 2. Create Partitioned Topic and Scale

**Prompt:**
> Create topic orders under public/default with 8 partitions; then scale to 16 partitions and show me the partition metadata.

**Possible triggers:**
`create-topic(partitions=8)` → `update-topic-partitions(16)` → `get-partitioned-metadata`

### 3. Clear Backlog and Compaction

**Prompt:**
> The backlog for public/default/orders is very large, clear it; then do a compaction.

**Possible triggers:**
`get-topic-backlog` → `expire-topic-messages/clear-namespace-backlog` → `compact-topic`

### 4. Schema Upload and Compatibility Testing

**Prompt:**
> Set Avro schema for persistent://public/default/orders: orderId:string, amount:double, and check compatibility.

**Possible triggers:**
`upload-schema(type=AVRO, schemaJson=...)` → `test-schema-compatibility` / `get-schema-info`

### 5. Subscription Management and Cursor Reset

**Prompt:**
> Create failover subscription sub-a on orders, then reset the cursor to 1 hour ago.

**Possible triggers:**
`create-subscription(type=failover)` → `reset-subscription-cursor(timestamp=now-1h)`

## Best Practices

- **Topic Naming**: Full names like `persistent://tenant/namespace/topic`. Short names are allowed, the server will normalize them.

- **Failure Domain**: Set Failure Domain for Broker/Bookie to improve rack/availability zone level disaster recovery.

- **Storage-Compute Separation**: Pulsar decouples storage (BookKeeper) from compute (Broker), making scaling and maintenance more flexible.

**Strategy Recommendations:**

- Set retention policies, backlog quotas, TTL at namespace level;
- Set partition count slightly higher than estimated for smoother future scaling;
- Recommend executing large expiration/cleanup operations during off-peak hours.

## Troubleshooting

### ObjectMapper must be set
Your MCP SDK version requires explicit ObjectMapper:

```java
var transport = HttpServletStreamableServerTransportProvider.builder()
    .objectMapper(new ObjectMapper().findAndRegisterModules())
    .build();
```

### NoClassDefFoundError: LogarithmicArrayByteBufferPool
Jetty version conflicts. Use Jetty 11 consistently:
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
And use `org.eclipse.jetty.ee9.servlet.*` imports.

### STDIO Mode JSON Polluted by Logs
Turn off/reduce stdout logging, output errors to stderr; stdout should only output MCP JSON.

### Message Send/Receive Unavailable
PulsarClient not initialized, or producer/consumer not created. Returns `not_implemented` when only Admin is available.

### Expire message … due to ongoing message expiration
Another expiration task is already running on the same partition; wait for completion or execute on partitions individually; observe with `get-topic-internal-stats`.

## Version Recommendations (Matrix)

| Component | Recommended Version |
|-----------|-------------------|
| Java | 17+ |
| Pulsar | 2.10+ (3.x preferred) |
| MCP Java SDK | 0.12.x |
| Jetty | 11.0.20 |
| Jackson | 2.17.2 |

## Development Guidelines

- **Layered Architecture**: ClusterTools / TenantTools / NamespaceTools / TopicTools / SubscriptionTools / SchemaTools / MessageTools / MonitoringTools

- **Client Management**: PulsarClientManager lazily loads and thread-safely manages PulsarAdmin and PulsarClient, shared across multiple tools.

- **Exception Handling**: Only return first line error messages, stack traces go to stderr; ensure MCP responses are 100% valid JSON.

- **Tool Filtering**: Control exported capabilities through `--allow-tools` / `--block-tools`.

## Testing and Code Standards

```bash
# Unit tests
mvn test

# Code style (optional)
mvn spotless:apply
mvn license:format
mvn checkstyle:check
```

## License

Apache License 2.0 (see [LICENSE](../../LICENSE) for details).