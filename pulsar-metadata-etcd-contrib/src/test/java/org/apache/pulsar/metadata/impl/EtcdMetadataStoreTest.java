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
package org.apache.pulsar.metadata.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.test.EtcdClusterExtension;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.testng.annotations.Test;

@Slf4j
public class EtcdMetadataStoreTest {

  private String getMetadataUrl(EtcdCluster etcdCluster) {
    return "etcd:"
        + etcdCluster.clientEndpoints().stream()
            .map(URI::toString)
            .collect(Collectors.joining(","));
  }

  private MetadataStore createStore(EtcdCluster etcdCluster) throws MetadataStoreException {
    String metadataURL = getMetadataUrl(etcdCluster);
    return new EtcdMetadataStore(metadataURL, MetadataStoreConfig.builder().build(), true);
  }

  @Test
  public void testBasicOperations() throws Exception {
    @Cleanup
    EtcdCluster etcdCluster =
        EtcdClusterExtension.builder()
            .withClusterName("test-basic")
            .withNodes(1)
            .withSsl(false)
            .build()
            .cluster();
    etcdCluster.start();

    @Cleanup MetadataStore store = createStore(etcdCluster);

    // Test put and get
    store.put("/test", "value".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
    assertTrue(store.exists("/test").join());

    Optional<GetResult> result = store.get("/test").join();
    assertTrue(result.isPresent());
    assertEquals(new String(result.get().getValue(), StandardCharsets.UTF_8), "value");

    // Test update
    store
        .put(
            "/test",
            "value2".getBytes(StandardCharsets.UTF_8),
            Optional.of(result.get().getStat().getVersion()))
        .join();
    result = store.get("/test").join();
    assertTrue(result.isPresent());
    assertEquals(new String(result.get().getValue(), StandardCharsets.UTF_8), "value2");

    // Test delete
    store.delete("/test", Optional.empty()).join();
    assertFalse(store.exists("/test").join());
  }

  @Test
  public void testGetChildren() throws Exception {
    @Cleanup
    EtcdCluster etcdCluster =
        EtcdClusterExtension.builder()
            .withClusterName("test-children")
            .withNodes(1)
            .withSsl(false)
            .build()
            .cluster();
    etcdCluster.start();

    @Cleanup MetadataStore store = createStore(etcdCluster);

    store.put("/parent/child1", "v1".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
    store.put("/parent/child2", "v2".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
    store.put("/parent/child3", "v3".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();

    List<String> children = store.getChildren("/parent").join();
    assertEquals(children.size(), 3);
    assertTrue(children.contains("child1"));
    assertTrue(children.contains("child2"));
    assertTrue(children.contains("child3"));
  }

  @Test
  public void testCluster() throws Exception {
    @Cleanup
    EtcdCluster etcdCluster =
        EtcdClusterExtension.builder()
            .withClusterName("test-cluster")
            .withNodes(3)
            .withSsl(false)
            .build()
            .cluster();
    etcdCluster.start();

    @Cleanup MetadataStore store = createStore(etcdCluster);

    store.put("/test", "value".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
    assertTrue(store.exists("/test").join());
  }

  @Test
  public void testClusterWithTls() throws Exception {
    @Cleanup
    EtcdCluster etcdCluster =
        EtcdClusterExtension.builder()
            .withClusterName("test-cluster-tls")
            .withNodes(3)
            .withSsl(true)
            .build()
            .cluster();
    etcdCluster.start();

    EtcdConfig etcdConfig =
        EtcdConfig.builder()
            .useTls(true)
            .tlsProvider(null)
            .authority("etcd0")
            .tlsTrustCertsFilePath(Resources.getResource("ssl/cert/ca.pem").getPath())
            .tlsKeyFilePath(Resources.getResource("ssl/cert/client-key-pk8.pem").getPath())
            .tlsCertificateFilePath(Resources.getResource("ssl/cert/client.pem").getPath())
            .build();

    Path etcdConfigPath = Files.createTempFile("etcd_config_cluster_ssl", ".yml");
    new ObjectMapper(new YAMLFactory()).writeValue(etcdConfigPath.toFile(), etcdConfig);

    String metadataURL = getMetadataUrl(etcdCluster);

    @Cleanup
    MetadataStore store =
        new EtcdMetadataStore(
            metadataURL,
            MetadataStoreConfig.builder().configFilePath(etcdConfigPath.toString()).build(),
            true);

    store.put("/test", "value".getBytes(StandardCharsets.UTF_8), Optional.empty()).join();
    assertTrue(store.exists("/test").join());
  }
}
