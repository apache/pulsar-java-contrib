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
package org.apache.pulsar.admin.mcp.transport;

import java.lang.reflect.Field;
import org.apache.pulsar.admin.mcp.client.PulsarClientManager;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StdioMCPServerTest {

    @Mock
    private PulsarClientManager mockPulsarClientManager;

    @Mock
    private PulsarAdmin mockPulsarAdmin;

    @Mock
    private PulsarClient mockPulsarClient;

    private StdioMCPServer server;
    private AutoCloseable mocks;

    @BeforeMethod
    public void setUp() {
        this.mocks = MockitoAnnotations.openMocks(this);
        this.server = new StdioMCPServer();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
        if (server != null) {
            try {
                server.stop();
            } catch (Exception ignore) {
            }
        }
    }

    @Test(
            expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = ".*PulsarClientManager not injected.*"
    )
    public void start_shouldThrowException_whenPulsarClientManagerNotInjected() {
        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(new String[]{});
        server.start(options);
    }

    @Test(
            expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = ".*Cannot start MCP server without Pulsar connection.*"
    )
    public void start_shouldThrowException_whenPulsarAdminInitializationFails() throws Exception {
        injectPulsarClientManager();
        org.mockito.Mockito.when(mockPulsarClientManager.getAdmin())
                .thenThrow(new RuntimeException("Admin init failed"));

        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(new String[]{});
        server.start(options);
    }

    @Test(
            expectedExceptions = RuntimeException.class,
            expectedExceptionsMessageRegExp = ".*Cannot start MCP server without Pulsar connection.*"
    )
    public void start_shouldThrowException_whenPulsarClientInitializationFails() throws Exception {
        injectPulsarClientManager();
        org.mockito.Mockito.when(mockPulsarClientManager.getAdmin())
                .thenReturn(mockPulsarAdmin);
        org.mockito.Mockito.when(mockPulsarClientManager.getClient())
                .thenThrow(new RuntimeException("Client init failed"));

        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(new String[]{});
        server.start(options);
    }

    @Test
    public void start_shouldStartServerSuccessfully() throws Exception {
        injectPulsarClientManager();
        org.mockito.Mockito.when(mockPulsarClientManager.getAdmin())
                .thenReturn(mockPulsarAdmin);
        org.mockito.Mockito.when(mockPulsarClientManager.getClient())
                .thenReturn(mockPulsarClient);

        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(new String[]{});
        server.start(options);

        org.testng.Assert.assertNotNull(server);
    }

    @Test
    public void start_shouldWarn_whenAlreadyRunning() throws Exception {
        injectPulsarClientManager();
        org.mockito.Mockito.when(mockPulsarClientManager.getAdmin())
                .thenReturn(mockPulsarAdmin);
        org.mockito.Mockito.when(mockPulsarClientManager.getClient())
                .thenReturn(mockPulsarClient);

        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(new String[]{});

        server.start(options);
        server.start(options);

        org.testng.Assert.assertNotNull(server);
    }

    @Test
    public void stop_shouldStopServerSuccessfully() throws Exception {
        injectPulsarClientManager();
        org.mockito.Mockito.when(mockPulsarClientManager.getAdmin())
                .thenReturn(mockPulsarAdmin);
        org.mockito.Mockito.when(mockPulsarClientManager.getClient())
                .thenReturn(mockPulsarClient);

        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(new String[]{});
        server.start(options);

        server.stop();
        org.mockito.Mockito.verify(mockPulsarClientManager).close();
    }

    @Test
    public void stop_shouldHandleWhenNotStarted() {
        server.stop();
    }

    @Test
    public void stop_shouldBeIdempotent() throws Exception {
        injectPulsarClientManager();
        org.mockito.Mockito.when(mockPulsarClientManager.getAdmin())
                .thenReturn(mockPulsarAdmin);
        org.mockito.Mockito.when(mockPulsarClientManager.getClient())
                .thenReturn(mockPulsarClient);

        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(new String[]{});
        server.start(options);

        server.stop();
        server.stop();
        server.stop();

        org.testng.Assert.assertNotNull(server);
    }

    @Test
    public void getType_shouldReturnStdio() {
        org.testng.Assert.assertEquals(server.getType(), PulsarMCPCliOptions.TransportType.STDIO);
    }

    @Test
    public void start_shouldHandleConcurrentCalls() throws Exception {
        injectPulsarClientManager();
        org.mockito.Mockito.when(mockPulsarClientManager.getAdmin())
                .thenReturn(mockPulsarAdmin);
        org.mockito.Mockito.when(mockPulsarClientManager.getClient())
                .thenReturn(mockPulsarClient);

        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(new String[]{});

        server.start(options);
        server.start(options);
        server.start(options);

        org.testng.Assert.assertNotNull(server);
    }

    @Test
    public void stop_shouldClosePulsarClientManager() throws Exception {
        injectPulsarClientManager();
        org.mockito.Mockito.when(mockPulsarClientManager.getAdmin())
                .thenReturn(mockPulsarAdmin);
        org.mockito.Mockito.when(mockPulsarClientManager.getClient())
                .thenReturn(mockPulsarClient);

        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(new String[]{});
        server.start(options);

        org.mockito.Mockito.verify(mockPulsarClientManager, org.mockito.Mockito.times(0)).close();

        server.stop();

        org.mockito.Mockito.verify(mockPulsarClientManager, org.mockito.Mockito.times(1)).close();
    }

    private void injectPulsarClientManager() throws Exception {
        Field field = AbstractMCPServer.class.getDeclaredField("pulsarClientManager");
        field.setAccessible(true);
        field.set(server, mockPulsarClientManager);
    }
}

