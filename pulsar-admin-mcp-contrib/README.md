# Pulsar Admin MCP Contrib

Apache Pulsar management server based on Model Context Protocol (MCP), enabling AI assistants to manage Pulsar clusters through a unified interface (supports both HTTP Streaming and STDIO transport modes).

## Demo
### Claude Desktop
![demo1](demo1.gif)

### Cherry Studio
![demo2](demo2.gif)

## Quick Start

### Dependencies

- Java 17+
- Maven 3.6+
- Pulsar 2.10+ (3.x preferred)
- MCP Java SDK 0.12.0
- Jetty 11.0.20

## 0. Start Pulsar

### Method A: Docker
```bash
docker run -it --name pulsar -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:3.2.4 bin/pulsar standalone
```

- **Service URL**: `pulsar://localhost:6650`
- **Admin URL**: `http://localhost:8080`

### Method B: Local Binary
```bash
bin/pulsar standalone
```

## 1. Build Pulsar-MCP

```bash
mvn clean install -DskipTests -am -pl pulsar-admin-mcp-contrib
```

**Output**: `target/mcp-contrib-1.0.0-SNAPSHOT.jar`

## 2. Start MCP Server

### HTTP Mode (Recommended: Web/Remote)
```bash
java -jar pulsar-admin-mcp-contrib/target/mcp-contrib-1.0.0-SNAPSHOT.jar --transport http --port 8889
```

**Log Example**:
```
HTTP Streamable transport ready at http://localhost:8889/mcp/stream
```

**Health Check**:
```bash
curl -i http://localhost:8889/mcp/stream
```

### STDIO Mode (Recommended: Claude Desktop / Local IDE)
```bash
java -jar pulsar-admin-mcp-contrib/target/mcp-contrib-1.0.0-SNAPSHOT.jar --transport stdio
```

## 3. Client Configuration

### Claude Desktop

#### Windows Configuration

**Config File Location**: `%APPDATA%\Claude\claude_desktop_config.json`

**Configuration Steps**:
1. Open Claude Desktop config file
2. Add the following configuration to the `mcpServers` section:

```json
{
  "mcpServers": {
    "pulsar-admin": {
      "command": "java",
      "args": ["-jar", "Path to compiled JAR, e.g., E:\\projects\\pulsar-admin-mcp-contrib\\target\\mcp-contrib-1.0.0-SNAPSHOT.jar", "--transport", "stdio"],
      "cwd": "Project directory, e.g., E:\\projects\\pulsar-admin-mcp-contrib",
      "env": {
        "PULSAR_SERVICE_URL": "pulsar://localhost:6650",
        "PULSAR_ADMIN_URL": "http://localhost:8080"
      }
    }
  }
}
```

#### macOS Configuration

**Config File Location**: `~/Library/Application Support/Claude/claude_desktop_config.json`

**Configuration Steps**:
1. Open Claude Desktop config file
2. Add the following configuration to the `mcpServers` section:

```json
{
  "mcpServers": {
    "pulsar-admin": {
      "command": "java",
      "args": ["-jar", "Path to compiled JAR, e.g., /Users/username/projects/pulsar-admin-mcp-contrib/target/mcp-contrib-1.0.0-SNAPSHOT.jar", "--transport", "stdio"],
      "cwd": "Project directory, e.g., /Users/username/projects/pulsar-admin-mcp-contrib",
      "env": {
        "PULSAR_SERVICE_URL": "pulsar://localhost:6650",
        "PULSAR_ADMIN_URL": "http://localhost:8080"
      }
    }
  }
}
```

**Notes**:
- Replace `Path to compiled JAR` with the actual JAR file path
- Replace `Project directory` with the actual project root directory path
- Ensure Java environment variables are properly configured
- Windows uses backslash `\`, macOS uses forward slash `/`

### Cherry Studio

#### STDIO Mode Configuration
Same as above

#### HTTP Mode Configuration

**Configuration Steps**:
1. Ensure MCP server is started (HTTP mode)
2. Add HTTP type configuration in Cherry Studio:

```json
{
  "mcpServers": {
    "pulsar-admin-http": {
      "type": "http",
      "url": "http://localhost:8889/mcp"
    }
  }
}
```

**Configuration Notes**:
- **STDIO Mode**: Suitable for local development, requires full JAR file path
- **HTTP Mode**: Suitable for remote access, requires HTTP server to be started first
- Both modes use the same environment variable configuration for Pulsar cluster connection
- Windows uses backslash `\`, macOS uses forward slash `/`

## 4. Configuration Options

### Environment Variables
- `PULSAR_SERVICE_URL` (default `pulsar://localhost:6650`)
- `PULSAR_ADMIN_URL` (default `http://localhost:8080`)

### Command Line Parameters
- `--transport`: http / stdio
- `--port`: HTTP port (default 8889)

## 5. Tool Inventory

Covers **Cluster** / **Tenant** / **Namespace** / **Topic** / **Subscription** / **Message** / **Schema** / **Monitoring** 8 categories, totaling 71 tools:

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

> **Note**: When only PulsarAdmin is initialized, message send/consume related tools will return `not_implemented`; to enable them, initialize PulsarClient and create producer/consumer.

## Natural Language Interaction Demo (Use Cases)

The following examples demonstrate typical workflows of triggering tool calls with natural language in MCP clients. Actual returned fields vary by cluster.

### 1. Tenant and Namespace Management

**Prompt:**
> Show me all tenants in the cluster; create namespace ns-orders under tenant1, then show me the statistics for this namespace.

**Triggered:**
`list-tenants` → `create-namespace(tenant=tenant1, namespace=ns-orders)` → `get-namespace-stats(...)`

### 2. Create Partitioned Topic and Scale

**Prompt:**
> Create topic orders under public/default with 8 partitions; then scale to 16 partitions and show me the partition metadata.

**Triggered:**
`create-topic(partitions=8)` → `update-topic-partitions(16)` → `get-partitioned-metadata`

### 3. Clear Backlog and Compact

**Prompt:**
> public/default/orders has a large backlog, clear it; then do a compaction.

**Triggered:**
`get-topic-backlog` → `expire-topic-messages/clear-namespace-backlog` → `compact-topic`

### 4. Schema Upload and Compatibility Testing

**Prompt:**
> Set Avro schema for persistent://public/default/orders: orderId:string, amount:double, and check compatibility.

**Triggered:**
`upload-schema(type=AVRO, schemaJson=...)` → `test-schema-compatibility` / `get-schema-info`

### 5. Subscription Management and Cursor Reset

**Prompt:**
> Create failover subscription sub-a on orders, then reset cursor to 1 hour ago.

**Triggered:**
`create-subscription(type=failover)` → `reset-subscription-cursor(timestamp=now-1h)`

## Best Practices

- **Topic Naming**: Full name format is `persistent://tenant/namespace/topic`. Short names are allowed, server will normalize.

- **Failure Domain**: Set Failure Domain for Broker/Bookie to improve rack/availability zone level disaster recovery.

## License

Apache License 2.0 (see [LICENSE](../../LICENSE) for details).