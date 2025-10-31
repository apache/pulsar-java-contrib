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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.springframework.stereotype.Component;

@Component
public class PulsarClientManager implements AutoCloseable {

    private PulsarAdmin pulsarAdmin;
    private PulsarClient pulsarClient;

    private final AtomicBoolean adminInitialized = new AtomicBoolean();
    private final AtomicBoolean clientInitialized = new AtomicBoolean();

    public void initialize() {
        getAdmin();
        getClient();
    }

    public synchronized PulsarAdmin getAdmin() {
        if (!adminInitialized.get()) {
            initializePulsarAdmin();
        }
        return pulsarAdmin;
    }

    public synchronized PulsarClient getClient() {
        if (!clientInitialized.get()) {
            initializePulsarClient();
        }
        return pulsarClient;
    }

    private void initializePulsarAdmin() {

        if (!adminInitialized.compareAndSet(false, true)) {
            return;
        }

        boolean success = false;
        try {
            String adminUrl = System.getenv().getOrDefault("PULSAR_ADMIN_URL", "http://localhost:8080");

            PulsarAdminBuilder adminBuilder = PulsarAdmin.builder()
                    .serviceHttpUrl(adminUrl)
                    .connectionTimeout(30, TimeUnit.SECONDS)
                    .readTimeout(60, TimeUnit.SECONDS);

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

    private void initializePulsarClient()  {
        if (!clientInitialized.compareAndSet(false, true)) {
            return;
        }
        boolean success = false;
        try {
            String serviceUrl = System.getenv().getOrDefault("PULSAR_SERVICE_URL", "pulsar://localhost:6650");

            var clientBuilder = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .operationTimeout(30, TimeUnit.SECONDS)
                    .connectionTimeout(30, TimeUnit.SECONDS)
                    .keepAliveInterval(30, TimeUnit.SECONDS);

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
