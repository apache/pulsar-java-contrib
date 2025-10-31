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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TransportManagerTest {

    private TransportManager mgr;
    private PulsarMCPCliOptions opts;

    @BeforeMethod
    public void setUp() {
        mgr = new TransportManager();
        opts = new PulsarMCPCliOptions();
    }

    @Test
    public void startTransport_shouldInvokeRegisteredTransport_withPassedOptions() throws Exception {
        Transport http = mock(Transport.class);
        when(http.getType()).thenReturn(PulsarMCPCliOptions.TransportType.HTTP);

        mgr.registerTransport(http);
        mgr.startTransport(PulsarMCPCliOptions.TransportType.HTTP, opts);

        verify(http, times(1)).start(opts);
        verify(http, times(1)).getType();
        verifyNoMoreInteractions(http);
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*Transport not registered.*"
    )
    public void startTransport_shouldThrow_whenTransportNotRegistered() throws Exception {
        mgr.startTransport(PulsarMCPCliOptions.TransportType.HTTP, opts);
    }

    @Test
    public void registerTransport_shouldOverridePrevious_whenSameTypeRegisteredAgain() throws Exception {
        Transport t1 = mock(Transport.class);
        when(t1.getType()).thenReturn(PulsarMCPCliOptions.TransportType.HTTP);

        Transport t2 = mock(Transport.class);
        when(t2.getType()).thenReturn(PulsarMCPCliOptions.TransportType.HTTP);

        mgr.registerTransport(t1);
        mgr.registerTransport(t2);

        mgr.startTransport(PulsarMCPCliOptions.TransportType.HTTP, opts);

        verify(t2, times(1)).start(opts);
        verify(t1, never()).start(any());
    }

    @Test
    public void registerTransport_shouldQueryTypeOnce_andStoreByType() {
        Transport any = mock(Transport.class);
        when(any.getType()).thenReturn(PulsarMCPCliOptions.TransportType.STDIO);

        mgr.registerTransport(any);

        verify(any, times(1)).getType();
        verifyNoMoreInteractions(any);
    }

    @Test
    public void registerTransport_shouldAllowMultipleDifferentTypes() throws Exception {
        Transport stdio = mock(Transport.class);
        when(stdio.getType()).thenReturn(PulsarMCPCliOptions.TransportType.STDIO);

        Transport http = mock(Transport.class);
        when(http.getType()).thenReturn(PulsarMCPCliOptions.TransportType.HTTP);

        mgr.registerTransport(stdio);
        mgr.registerTransport(http);

        mgr.startTransport(PulsarMCPCliOptions.TransportType.STDIO, opts);
        mgr.startTransport(PulsarMCPCliOptions.TransportType.HTTP, opts);

        verify(stdio, times(1)).start(opts);
        verify(http, times(1)).start(opts);
    }

    @Test
    public void startTransport_shouldWorkAfterMultipleRegistrations() throws Exception {
        Transport transport1 = mock(Transport.class);
        when(transport1.getType()).thenReturn(PulsarMCPCliOptions.TransportType.HTTP);

        Transport transport2 = mock(Transport.class);
        when(transport2.getType()).thenReturn(PulsarMCPCliOptions.TransportType.HTTP);

        mgr.registerTransport(transport1);
        mgr.registerTransport(transport2);

        mgr.startTransport(PulsarMCPCliOptions.TransportType.HTTP, opts);

        verify(transport2, times(1)).start(opts);
        verify(transport1, never()).start(any());
    }

    @Test
    public void startTransport_shouldPassNullOptionsToTransport() throws Exception {
        Transport http = mock(Transport.class);
        when(http.getType()).thenReturn(PulsarMCPCliOptions.TransportType.HTTP);

        mgr.registerTransport(http);
        mgr.startTransport(PulsarMCPCliOptions.TransportType.HTTP, null);

        verify(http, times(1)).start(null);
    }

    @Test
    public void startTransport_shouldCallStartMethod_afterRegistration() throws Exception {
        Transport stdio = mock(Transport.class);
        when(stdio.getType()).thenReturn(PulsarMCPCliOptions.TransportType.STDIO);

        mgr.registerTransport(stdio);
        mgr.startTransport(PulsarMCPCliOptions.TransportType.STDIO, opts);

        verify(stdio, times(1)).start(opts);
    }

    @Test
    public void registerTransport_shouldHandleConcurrentRegistration() {
        Transport transport1 = mock(Transport.class);
        Transport transport2 = mock(Transport.class);
        when(transport1.getType()).thenReturn(PulsarMCPCliOptions.TransportType.HTTP);
        when(transport2.getType()).thenReturn(PulsarMCPCliOptions.TransportType.STDIO);

        mgr.registerTransport(transport1);
        mgr.registerTransport(transport2);

        verify(transport1, times(1)).getType();
        verify(transport2, times(1)).getType();
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*Transport not registered.*"
    )
    public void startTransport_shouldThrow_whenTransportTypeNotRegistered() throws Exception {
        mgr.startTransport(PulsarMCPCliOptions.TransportType.HTTP, opts);
    }
}
