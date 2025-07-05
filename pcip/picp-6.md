# PCIP-6: Implement the MCP for Pulsar Admin Tool

# Background knowledge

Apache Pulsar is a cloud-native distributed messaging and streaming platform, offering unified messaging, storage, and stream processing. It features multi-tenancy, persistent storage, and seamless scalability. Pulsar provides a command-line tool `pulsar-admin` to manage clusters, tenants, namespaces, topics, and subscriptions. However, `pulsar-admin` requires users to learn a large number of CLI commands and options, which is not user-friendly for non-experts.

Model Context Protocol (MCP) is an open standard that defines how external tools expose capabilities to large language models (LLMs) through structured tool schemas. MCP allows LLMs like GPT or Claude to interact with systems such as Pulsar through well-defined interfaces. This is analogous to USB-C as a standardized interface for hardware devices.

# Motivation

Although `pulsar-admin` provides comprehensive CLI access, it's not user-friendly for non-experts. Users often struggle to express complex management needs, such as "list all subscriptions with message backlog > 1000", using raw CLI syntax.

This proposal aims to introduce a new module that exposes Pulsar Admin functionalities as MCP-compatible tools. This allows users to manage Pulsar clusters using natural language prompts, interpreted and executed by LLMs.

Benefits include:

- Simplified cluster operations using plain English or other natural languages
- Multi-turn conversational interactions with context awareness
- A clean, extensible tool layer that conforms to the `pulsar-java-contrib` architecture


# Goals

## In Scope

- Implement a new module `pulsar-admin-mcp-contrib` to expose Pulsar admin functionalities via MCP protocol.
- Support 70+ commonly used admin operations via structured MCP tools.
- Provide built-in transport support for HTTP, STDIO, and SSE.
- Implement robust context tracking and parameter validation.
- Provide complete developer/user documentation and a demo use case.

## Out of Scope

- UI interfaces or web frontends (though it can be used as backend).
- Pulsar core changes (only builds on `pulsar-java-contrib`).
- Full support for all 120+ CLI commands (70+ are targeted for now).

# High Level Design

The system architecture consists of four main layers:

1. **LLM Interface Layer**: Accepts natural language inputs from an LLM and generates MCP-compatible tool calls
2. **Protocol Layer**: Central MCP interface that dispatches structured requests to tool handlers
3. **Tool Execution Layer**: Looks up registered tools and invokes appropriate Pulsar Admin API operations
4. **Context Management Layer**: Maintains session memory, allowing parameter inheritance across steps

Example interaction:

User input:  
> "Create a topic named `user-events` with 3 partitions"

LLMs send structured tool calls (as per MCP schema) such as:
```json
{
  "tool": "create-topic",
  "parameters": {
    "name": "user-events",
    "partitions": 3
  }
}
```

MCP executes the tool and returns a structured result, which the LLM then summarizes in natural language.
# Detailed Design

## Design & Implementation Details

### Package structure:
```java
pulsar-java-contrib/
	├── MCPProtocol.java           # MCP 协议接口
	├── MCPFactory.java            # 工厂类，动态加载协议实例
	├── tools/                     # 工具注册与具体实现
	├── client/                    # 管理 PulsarAdmin 客户端
	├── context/                   # 会话状态管理
	├── validation/                # 参数验证机制
	├── transport/                 # 支持 HTTP、STDIO、SSE
	├── model/                     # ToolSchema、ToolResult 等核心数据结构
```

### Key components:
- `PulsarAdminTool`: Abstract base class for all tools (e.g. list-topics, create-tenant)
- `ToolExecutor`: Handles concurrency, thread pools, and context updates
- `ToolRegistry`: Registers all tools via Java SPI
- `SessionManager`: Tracks ongoing sessions and enhances parameters
- `ParameterValidator`: Validates tool input against ToolSchema metadata

###  Supported tools

**Total tools**: 70+  
Grouped by category:
- **Cluster**: list-clusters, create-cluster, get-cluster-stats
- **Tenant**: list-tenants, create-tenant, delete-tenant
- **Namespace**: create-namespace, set-retention-policy, list-namespaces
- **Topic**: create-topic, delete-topic, list-topics, compact-topic, get-topic-stats
- **Subscription**: reset-subscription, get-subscription-stats
- **Message**: produce-message, peek-messages
- **Schema**: upload-schema, check-schema-compatibility
- **Monitoring**: get-cluster-performance, diagnose-topic

Each tool includes:

- A ToolSchema (for LLM prompt templates and validation)
- A handler (e.g. `ListTopicsHandler`)
- Parameter schema, default values, error messages

