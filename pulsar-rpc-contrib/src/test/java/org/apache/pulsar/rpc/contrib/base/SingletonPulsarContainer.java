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
package org.apache.pulsar.rpc.contrib.base;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class SingletonPulsarContainer {

  private static final PulsarContainer PULSAR_CONTAINER;

  static {
    PULSAR_CONTAINER =
        new PulsarContainer(getPulsarImage())
            .withEnv("PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled", "true")
            .withEnv("PULSAR_PREFIX_delayedDeliveryEnabled", "true")
            .withStartupTimeout(Duration.ofMinutes(3));
    PULSAR_CONTAINER.start();
  }

  private static DockerImageName getPulsarImage() {
    return DockerImageName.parse("apachepulsar/pulsar:" + getPulsarImageVersion());
  }

  private static String getPulsarImageVersion() {
    String pulsarVersion = "";
    Properties properties = new Properties();
    try {
      properties.load(
          SingletonPulsarContainer.class
              .getClassLoader()
              .getResourceAsStream("pulsar-container.properties"));
      if (!properties.isEmpty()) {
        pulsarVersion = properties.getProperty("pulsar.version");
      }
    } catch (IOException e) {
      log.error("Failed to load pulsar version. " + e.getCause());
    }
    return pulsarVersion;
  }

  static PulsarClient createPulsarClient() throws PulsarClientException {
    return PulsarClient.builder()
        .serviceUrl(SingletonPulsarContainer.PULSAR_CONTAINER.getPulsarBrokerUrl())
        .build();
  }

  static PulsarAdmin createPulsarAdmin() throws PulsarClientException {
    return PulsarAdmin.builder()
        .serviceHttpUrl(SingletonPulsarContainer.PULSAR_CONTAINER.getHttpServiceUrl())
        .build();
  }
}
