# Apache Pulsar Plugin Contribution Guide

## Welcome
Thank you for your contribution to the Apache Pulsar project. This document will guide you to understand and implement Pulsar's extensible interface.

## 1. Introduction
Apache Pulsar is a distributed messaging and streaming platform. You can view detailed information [here](pulsar.apache.org/).
Pulsar allows users to customize plugins and integrate them into Pulsar to meet customized needs by exposing interfaces and configurations.
Many of these plugins are similar and can be reused. This project aims to collect and organize the implementation of various plugins, reduce the development cost caused by repeated implementations in the community, and lower the threshold for using Pulsar.

## 2. Core interface list
List the core interfaces in Pulsar that can be implemented by contributors.

### 2.1 Pulsar client common extension interface
- `org.apache.pulsar.client.api.MessageListenerExecutor.java`

### 2.2 Authentication and authorization related interfaces
- `org.apache.pulsar.broker.authorization.AuthorizationProvider`
- `org.apache.pulsar.client.api.AuthenticationDataProvider`
- `org.apache.pulsar.functions.auth.FunctionAuthProvider`
- `org.apache.pulsar.functions.auth.KubernetesFunctionAuthProvider`
- `org.apache.pulsar.functions.secretsprovider.SecretsProvider`

### 2.3 Transaction-related interfaces
- `org.apache.pulsar.transaction.coordinator.TransactionMetadataStoreProvider`
- `org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider`
- `org.apache.pulsar.broker.transaction.pendingack.TransactionPendingAckStoreProvider`

### 2.4 Load Balancer Extension Interface
- `org.apache.pulsar.broker.loadbalance.ModularLoadManager`
- `org.apache.pulsar.common.naming.TopicBundleAssignmentStrategy`
- `org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy`
- `org.apache.pulsar.broker.loadbalance.ModularLoadManagerStrategy`
- `org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerFilter`

### 2.5 Interceptor Interface
- `org.apache.pulsar.client.api.ProducerInterceptor`
- `org.apache.pulsar.client.api.ConsumerInterceptor`
- `org.apache.pulsar.client.api.ReaderInterceptor`
- `org.apache.pulsar.broker.intercept.BrokerInterceptor`
- `org.apache.pulsar.broker.service.TopicEventsListener`
- `org.apache.pulsar.client.api.ConsumerEventListener`
- `org.apache.pulsar.client.impl.transaction.TransactionBufferHandler`

### 2.6 Pulsar Connector Interface
- `org.apache.pulsar.io.core.Sink`
- `org.apache.pulsar.io.core.Source`

### 2.7 Pulsar Function Interface
- `org.apache.pulsar.functions.api.Function`

### 2.8 Bookkeeper related interfaces
- `org.apache.pulsar.broker.service.schema.SchemaStorageFactory`
- `org.apache.pulsar.packages.management.core.PackagesStorageProvider`
- `org.apache.bookkeeper.mledger.ManagedLedger`

### 2.9 Metrics related interfaces
- `org.apache.pulsar.broker.stats.prometheus.PrometheusRawMetricsProvider`

## 3. Interface Implementation Guide
Provide implementation guide for each type of interfaces.

### 3.1 Client common extension interface implementation
- `org.apache.pulsar.client.api.MessageListenerExecutor.java`
- **Purpose**: Select different message processing threads according to business scenarios.
- **Sample code**: org.apache.pulsar.client.api.impl.KeySharedMessageListenerExecutor, org.apache.pulsar.client.api.impl.CommonMessageListenerExecutor, org.apache.pulsar.client.api.impl.PartitionOrderMessageListenerExecutor

### 3.2 Authentication and authorization related interfaces
- **Purpose**: Currently Pulsar has only a few default implementations for authentication and authorization interfaces. Users can customize the required authentication and authorization implementation through this interface.
- **Sample code**: https://github.com/apache/pulsar/tree/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authorization, https://github.com/apache/pulsar/tree/master/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/authentication

### 3.3 Implementation of transaction-related interfaces
- **Purpose**: Customize transaction components according to different business requirements. For example, the Transaction Buffer implemented based on the Exactly-once requirement may have different considerations and different implementations.
- **Sample code**: https://github.com/apache/pulsar/tree/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/transaction/buffer/impl, https://github.com/apache/pulsar/tree/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/transaction/pendingack, https://github.com/apache/pulsar/tree/master/pul sar-transaction/coordinator/src/main/java/org/apache/pulsar/transaction/coordinator
### 3.4 Load balancer extension interface implementation
- **Purpose**: According to the business scenario of the user, when the existing load balance strategy cannot meet the business needs or is not the best strategy, you can inherit the existing strategy and modify it or completely customize the load balancer strategy suitable for your business.
- **Sample code**: See the official existing implementation.https://github.com/apache/pulsar/tree/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/loadbalance

### 3.5 Interceptor extension interface implementation
- **Purpose**: Users can implement Pulsar's interceptor interface to perform logging and auditing, message conversion and filtering. These interceptors often have similar implementations and can be abstracted and reused. For example, a series of interceptors for logging can be fully reused.
- **Sample code**: None

### 3.6 Connector interface implementation
- **Purpose**: Connect Pulsar with external systems to import and export data.
- **Sample code**: https://github.com/apache/pulsar/tree/master/pulsar-io

### 3.7 Pulsar function interface implementation
- **Purpose**: Implement serverless computing logic to respond to data flow changes in Pulsar.
- **Sample code**: https://github.com/apache/pulsar/tree/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples

### 3.8 Bookkeeper related interface implementation
- **Purpose**: Processing logic related to persistent data
- **Sample code**: https://github.com/apache/pulsar/tree/master/managed-ledger/src/main/java/org/apache/bookkeeper/mledger/impl, https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/schema/BookkeeperSchemaStorageFactory.java,

### 3.9 Metrics related interface
- **Purpose**: Used to customize the generation of metric data in the Prometheus monitoring system
- **Sample code**:
```java
PrometheusRawMetricsProvider rawMetricsProvider = stream -> stream.write("test_metrics{label1=\"xyz\"} 10 \n"); 
```