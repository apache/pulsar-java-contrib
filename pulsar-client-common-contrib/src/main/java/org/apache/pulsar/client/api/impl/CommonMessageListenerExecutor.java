package org.apache.pulsar.client.api.impl;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListenerExecutor;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CommonMessageListenerExecutor implements MessageListenerExecutor {
    private final ExecutorService executorService;

    public CommonMessageListenerExecutor(int numThreads, String subscriptionName) {
        this.executorService = new ThreadPoolExecutor(numThreads, numThreads, 10000L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
                    private final AtomicInteger threadId = new AtomicInteger(0);

                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, subscriptionName + "-listener-executor-" + threadId.incrementAndGet());
                    }
                });
    }

    @Override
    public void execute(Message<?> message, Runnable runnable) {
        this.executorService.execute(runnable);
    }
}
