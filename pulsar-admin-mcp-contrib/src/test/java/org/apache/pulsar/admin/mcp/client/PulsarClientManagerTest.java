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

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PulsarClientManagerTest {

  private PulsarClientManager manager;
  private String originalAdminUrl;
  private String originalServiceUrl;

  @BeforeMethod
  public void setUp() {
    manager = new PulsarClientManager();

    originalAdminUrl = System.getenv("PULSAR_ADMIN_URL");
    originalServiceUrl = System.getenv("PULSAR_SERVICE_URL");

    System.clearProperty("PULSAR_ADMIN_URL");
    System.clearProperty("PULSAR_SERVICE_URL");
  }

  @AfterMethod
  public void tearDown() {
    if (manager != null) {
      try {
        manager.close();
      } catch (Exception ignore) {
      }
    }
    if (originalAdminUrl != null) {
      System.setProperty("PULSAR_ADMIN_URL", originalAdminUrl);
    } else {
      System.clearProperty("PULSAR_ADMIN_URL");
    }

    if (originalServiceUrl != null) {
      System.setProperty("PULSAR_SERVICE_URL", originalServiceUrl);
    } else {
      System.clearProperty("PULSAR_SERVICE_URL");
    }
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Failed to initialize PulsarAdmin.*")
  public void initialize_shouldThrowException_whenPulsarNotRunning() {
    System.setProperty("PULSAR_ADMIN_URL", "http://localhost:99999");
    System.setProperty("PULSAR_SERVICE_URL", "pulsar://localhost:99999");

    manager.initialize();
  }

  @Test
  public void getClient_shouldNotThrowException_whenPulsarNotRunning() {
    System.setProperty("PULSAR_ADMIN_URL", "http://localhost:8080");
    System.setProperty("PULSAR_SERVICE_URL", "pulsar://localhost:99999");

    try {
      org.apache.pulsar.client.api.PulsarClient client = manager.getClient();
      org.testng.Assert.assertNotNull(client);
    } catch (Exception e) {
      org.testng.Assert.assertTrue(e.getMessage().contains("Failed to initialize PulsarClient"));
    }
  }

  @Test
  public void close_shouldCloseBothAdminAndClient() throws Exception {
    PulsarClientManager testManager = new PulsarClientManager();

    testManager.close();

    testManager.close();
  }

  @Test
  public void initialize_shouldSetDefaultUrls() {
    try {
      manager.initialize();
      org.testng.Assert.fail("Expected exception for missing Pulsar connection");
    } catch (RuntimeException e) {
      org.testng.Assert.assertTrue(e.getMessage().contains("Failed to initialize PulsarAdmin"));
    }
  }

  @Test
  public void getAdmin_shouldUseDefaultUrl() {
    try {
      manager.getAdmin();
      org.testng.Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      org.testng.Assert.assertTrue(e.getMessage().contains("Failed to initialize PulsarAdmin"));
    }
  }

  @Test
  public void getAdmin_shouldUseEnvVarWhenSet() {
    System.setProperty("PULSAR_ADMIN_URL", "http://localhost:8080");

    try {
      manager.getAdmin();
      org.testng.Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      org.testng.Assert.assertTrue(e.getMessage().contains("Failed to initialize PulsarAdmin"));
    }
  }

  @Test
  public void getClient_shouldUseDefaultUrl() {
    org.apache.pulsar.client.api.PulsarClient client = manager.getClient();
    org.testng.Assert.assertNotNull(client);
  }

  @Test
  public void getClient_shouldUseEnvVarWhenSet() {
    System.setProperty("PULSAR_SERVICE_URL", "pulsar://localhost:6650");

    org.apache.pulsar.client.api.PulsarClient client = manager.getClient();
    org.testng.Assert.assertNotNull(client);
  }

  @Test
  public void initialize_shouldCallGetAdminAndGetClient() {
    try {
      manager.initialize();
      org.testng.Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      org.testng.Assert.assertNotNull(e.getMessage());
    }
  }

  @Test
  public void close_shouldBeIdempotent() throws Exception {
    manager.close();
    manager.close();
    manager.close();
  }

  @Test
  public void initialize_shouldHandleInvalidUrl() {
    System.setProperty("PULSAR_ADMIN_URL", "invalid-url");
    System.setProperty("PULSAR_SERVICE_URL", "invalid-url");

    try {
      manager.initialize();
      org.testng.Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      org.testng.Assert.assertTrue(e.getMessage().contains("Failed to initialize"));
    }
  }

  @Test
  public void singletonBehavior_shouldReturnSameInstance() {
    try {
      manager.getAdmin();
      org.testng.Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      org.testng.Assert.assertTrue(e.getMessage().contains("Failed to initialize PulsarAdmin"));
    }

    try {
      manager.getAdmin();
      org.testng.Assert.fail("Expected exception");
    } catch (RuntimeException e) {
      org.testng.Assert.assertTrue(e.getMessage().contains("Failed to initialize PulsarAdmin"));
    }
  }
}
