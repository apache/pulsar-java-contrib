package org.apache.pulsar.client.api.impl;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;
import org.apache.pulsar.client.util.ExecutorProvider;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

public class PartitionOrderMessageListenerExecutor implements MessageListenerExecutor {
    private final ExecutorProvider executorProvider;

    public PartitionOrderMessageListenerExecutor(int numThreads, String subscriptionName) {
        this.executorProvider = new ExecutorProvider(numThreads, subscriptionName + "listener-executor-");
    }

    @Override
    public void execute(Message<?> message, Runnable runnable) {
        // select a thread by partition topic name to execute the runnable!
        // that say, the message listener task from the same partition topic
        // will be executed by the same thread
        ExecutorService executorService = executorProvider.getExecutor(message.getTopicName().getBytes());
        // executorService is a SingleThreadExecutor
        executorService.execute(runnable);
    }
}
