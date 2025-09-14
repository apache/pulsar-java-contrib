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
    private final AtomicBoolean adminInitialized = new AtomicBoolean();
    private final AtomicBoolean clientInitialized = new AtomicBoolean();

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
        initializePulsarClient();
        return pulsarClient;
    }

    private void initializePulsarAdmin() throws Exception {

        if (!adminInitialized.compareAndSet(false, true)) {
            return;
        }

        boolean success = false;
        try {
            String adminUrl = (config != null && config.getAdminUrl() != null && !config.getAdminUrl().isBlank())
                    ? config.getAdminUrl()
                    : System.getenv().getOrDefault("PULSAR_ADMIN_URL", "http://localhost:8080");

            PulsarAdminBuilder adminBuilder = PulsarAdmin.builder()
                    .serviceHttpUrl(adminUrl)
                    .connectionTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(60, TimeUnit.SECONDS);

            String authPlugin = (config != null) ? config.getAuthPlugin() : null;
            String authParams = (config != null) ? config.getAuthParams() : null;
            if (authPlugin == null) {
                authPlugin = System.getProperty("pulsar.auth.plugin");
            }
            if (authParams == null) {
                authParams = System.getProperty("pulsar.auth.params");
            }

            if (authPlugin != null && authParams != null) {
                adminBuilder.authentication(authPlugin, authParams);
                LOGGER.info("Authentication configured: {}", authPlugin);
            }

            pulsarAdmin = adminBuilder.build();

            pulsarAdmin.clusters().getClusters();
            success = true;

        } catch (Exception e) {

            if (pulsarAdmin != null) {
                try {
                    pulsarAdmin.close();
                } catch (Exception ignore) {

                }
                pulsarAdmin = null;
            }
            adminInitialized.set(false);
            throw new RuntimeException("Failed to initialize PulsarAdmin", e);
        } finally {
            if (!success) {
                adminInitialized.set(false);
            }
        }
    }

    private void initializePulsarClient() throws Exception {
        if (!clientInitialized.compareAndSet(false, true)) {
            return;
        }
        boolean success = false;
        try {
            String serviceUrl = (config != null && config.getServiceUrl() != null && !config.getServiceUrl().isBlank())
                    ? config.getServiceUrl()
                    : System.getenv().getOrDefault("PULSAR_SERVICE_URL", "pulsar://localhost:6650");

            var clientBuilder = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .operationTimeout(30, TimeUnit.SECONDS)
                    .connectionTimeout(30, TimeUnit.SECONDS)
                    .keepAliveInterval(30, TimeUnit.SECONDS);

            String authPlugin = (config != null) ? config.getAuthPlugin() : null;
            String authParams = (config != null) ? config.getAuthParams() : null;
            if (authPlugin == null) {
                authPlugin = System.getProperty("pulsar.auth.plugin");
            }
            if (authParams == null) {
                authParams = System.getProperty("pulsar.auth.params");
            }
            if (authPlugin != null && authParams != null) {
                clientBuilder.authentication(authPlugin, authParams);
            }

            this.pulsarClient = clientBuilder.build();
            success = true;

        } catch (Exception e) {
            if (pulsarClient != null) {
                try {
                    pulsarClient.close();
                } catch (Exception ignore) {
                }
                pulsarClient = null;
            }
            clientInitialized.set(false);
            throw new RuntimeException("Failed to initialize PulsarClient", e);
        } finally {
            if (!success) {
                clientInitialized.set(false);
            }
        }
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
