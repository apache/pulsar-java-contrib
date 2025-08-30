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
package org.apache.pulsar.admin.mcp.client;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PulsarClientManager {

    private static final Logger logger = LoggerFactory.getLogger(PulsarClientManager.class);

    private PulsarAdmin pulsarAdmin;
    private PulsarClient pulsarClient;

    @Getter
    private final PulsarMCPCliOptions config;
    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    @Autowired
    public PulsarClientManager(PulsarMCPCliOptions config){
        this.config = config;
    }

    public synchronized void initialize() throws  Exception {
        if (initialized.get()) {
            return;
        }

        if (closed.get()) {
            return;
        }

        try {
            initializePulsarAdmin();

            initializePulsarClient();

            initialized.set(true);
        } catch (Exception e) {
            cleanup();
            throw new RuntimeException("Failed to connect to Pulsar cluster", e);
        }
    }

    private void initializePulsarAdmin() throws Exception {
        PulsarAdminBuilder adminBuilder = PulsarAdmin.builder()
                .serviceHttpUrl(config.getAdminUrl())
                .connectionTimeout(30, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS);

        if (config.getAuthPlugin() != null && config.getAuthParams() != null) {
            adminBuilder.authentication(config.getAuthPlugin(), config.getAuthParams());
        }

        this.pulsarAdmin = adminBuilder.build();
    }

    private void initializePulsarClient() throws Exception {
        var clientBuilder = PulsarClient.builder()
                .serviceUrl(config.getServiceUrl())
                .operationTimeout(30, TimeUnit.SECONDS)
                .connectionTimeout(30, TimeUnit.SECONDS)
                .keepAliveInterval(30,  TimeUnit.SECONDS);

        if (config.getAuthPlugin() != null && config.getAuthParams() != null) {
            clientBuilder.authentication(config.getAuthPlugin(), config.getAuthParams());
        }

        this.pulsarClient = clientBuilder.build();

    }

    public PulsarAdmin getPulsarAdmin() throws Exception {
        if (!initialized.get()) {
            initialize();
        }
        return pulsarAdmin;
    }

    public PulsarClient getPulsarClient() throws Exception {
        if (!initialized.get()) {
            initialize();
        }
        return pulsarClient;
    }

    public boolean isInitialized() {
        return initialized.get();
    }

    private void cleanup() {
        if (pulsarClient != null) {
            try {
                pulsarClient.close();
            } catch (Exception e) {
                logger.warn("Failed to close PulsarClient: {}", e.getMessage());
            }
            pulsarClient = null;
        }

        if (pulsarAdmin != null) {
            try {
                pulsarAdmin.close();
            } catch (Exception e) {
                logger.warn("Failed to close PulsarAdmin: {}", e.getMessage());
            }
        }
        initialized.set(false);
    }
}
