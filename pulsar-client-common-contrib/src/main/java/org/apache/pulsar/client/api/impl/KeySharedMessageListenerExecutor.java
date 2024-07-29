package org.apache.pulsar.client.api.impl;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;
import org.apache.pulsar.client.util.ExecutorProvider;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;

public class KeySharedMessageListenerExecutor implements MessageListenerExecutor {
    ExecutorProvider executorProvider;
    public KeySharedMessageListenerExecutor(int numThreads, String subscriptionName){
        this.executorProvider = new ExecutorProvider(numThreads, subscriptionName + "listener-executor-");
    }

    @Override
    public void execute(Message<?> message, Runnable runnable) {
        byte[] key = "".getBytes(StandardCharsets.UTF_8);
        if (message.hasKey()) {
            key = message.getKeyBytes();
        } else if (message.hasOrderingKey()) {
            key = message.getOrderingKey();
        }
        // select a thread by message key to execute the runnable!
        // that say, the message listener task with same order key
        // will be executed by the same thread
        ExecutorService executorService = executorProvider.getExecutor(key);
        // executorService is a SingleThreadExecutor
        executorService.execute(runnable);
    }
}
