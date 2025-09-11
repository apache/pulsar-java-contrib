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
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PulsarClientManager implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarClientManager.class);

    private PulsarAdmin pulsarAdmin;
    private PulsarClient pulsarClient;
    private final PulsarMCPCliOptions config;
    private final AtomicBoolean adminInitialized = new AtomicBoolean(false);
    private final AtomicBoolean clientInitialized = new AtomicBoolean(false);

    public PulsarClientManager(PulsarMCPCliOptions config){
        this.config = config;
    }

    public synchronized PulsarAdmin getAdmin() throws Exception {
        if (!adminInitialized.get()) {
            initializePulsarAdmin();
        }
        return pulsarAdmin;
    }

    public synchronized PulsarClient getClient() throws Exception {
        if (!clientInitialized.get()) {
            initializePulsarClient();
        }
        return pulsarClient;
    }

    private void initializePulsarAdmin() throws Exception {
        if (adminInitialized.get()) {
            return;
        }

        PulsarAdminBuilder adminBuilder = PulsarAdmin.builder()
                .serviceHttpUrl(config.getAdminUrl())
                .connectionTimeout(30, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS);

        if (config.getAuthPlugin() != null && config.getAuthParams() != null) {
            adminBuilder.authentication(config.getAuthPlugin(), config.getAuthParams());
        }

        this.pulsarAdmin = adminBuilder.build();
        pulsarAdmin.clusters().getClusters();
        adminInitialized.set(true);
    }

    private void initializePulsarClient() throws Exception {
        if (clientInitialized.get()) {
            return;
        }

        var clientBuilder = PulsarClient.builder()
                .serviceUrl(config.getServiceUrl())
                .operationTimeout(30, TimeUnit.SECONDS)
                .connectionTimeout(30, TimeUnit.SECONDS)
                .keepAliveInterval(30,  TimeUnit.SECONDS);

        if (config.getAuthPlugin() != null && config.getAuthParams() != null) {
            clientBuilder.authentication(config.getAuthPlugin(), config.getAuthParams());
        }

        this.pulsarClient = clientBuilder.build();
        clientInitialized.set(true);

    }

    @Override
    public void close() throws Exception {
        if (pulsarClient != null) {
            pulsarClient.close();
        }
        if (pulsarAdmin != null) {
            pulsarAdmin.close();
        }
        adminInitialized.set(false);
        clientInitialized.set(false);
    }

}
