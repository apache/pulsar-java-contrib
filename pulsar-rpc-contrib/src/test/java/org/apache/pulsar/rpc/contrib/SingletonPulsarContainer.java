package org.apache.pulsar.rpc.contrib;

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
        PULSAR_CONTAINER = new PulsarContainer(getPulsarImage())
                        .withEnv("PULSAR_PREFIX_acknowledgmentAtBatchIndexLevelEnabled", "true")
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
            properties.load(SingletonPulsarContainer.class.getClassLoader()
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
