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
package org.apache.pulsar.admin.mcp.config;

import org.testng.annotations.Test;

public class PulsarMCPCliOptionsTest {

    @Test
    public void parseArgs_shouldParseDefaultOptions() {
        String[] args = {};
        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);

        org.testng.Assert.assertEquals(options.getTransport(), PulsarMCPCliOptions.TransportType.STDIO);
        org.testng.Assert.assertEquals(options.getHttpPort(), 8889);
    }

    @Test
    public void parseArgs_shouldParseTransportOption() {
        String[] args = {"--transport", "http"};
        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);

        org.testng.Assert.assertEquals(options.getTransport(), PulsarMCPCliOptions.TransportType.HTTP);
        org.testng.Assert.assertEquals(options.getHttpPort(), 8889);
    }

    @Test
    public void parseArgs_shouldParseTransportOptionWithShortForm() {
        String[] args = {"-t", "stdio"};
        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);

        org.testng.Assert.assertEquals(options.getTransport(), PulsarMCPCliOptions.TransportType.STDIO);
        org.testng.Assert.assertEquals(options.getHttpPort(), 8889);
    }

    @Test
    public void parseArgs_shouldParsePortOption() {
        String[] args = {"--port", "9999"};
        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);

        org.testng.Assert.assertEquals(options.getTransport(), PulsarMCPCliOptions.TransportType.STDIO);
        org.testng.Assert.assertEquals(options.getHttpPort(), 9999);
    }

    @Test
    public void parseArgs_shouldParseBothOptions() {
        String[] args = {"--transport", "http", "--port", "8080"};
        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);

        org.testng.Assert.assertEquals(options.getTransport(), PulsarMCPCliOptions.TransportType.HTTP);
        org.testng.Assert.assertEquals(options.getHttpPort(), 8080);
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Missing value for --transport"
    )
    public void parseArgs_shouldThrowException_whenTransportValueMissing() {
        PulsarMCPCliOptions.parseArgs(new String[]{"--transport"});
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Missing value for --port"
    )
    public void parseArgs_shouldThrowException_whenPortValueMissing() {
        PulsarMCPCliOptions.parseArgs(new String[]{"--port"});
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*invalid is not a valid TransportType.*Valid Options: stdio,http.*"
    )
    public void parseArgs_shouldThrowException_whenInvalidTransport() {
        PulsarMCPCliOptions.parseArgs(new String[]{"--transport", "invalid"});
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Invalid port number for --port"
    )
    public void parseArgs_shouldThrowException_whenInvalidPort() {
        PulsarMCPCliOptions.parseArgs(new String[]{"--port", "invalid"});
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "Unknown argument: --unknown"
    )
    public void parseArgs_shouldThrowException_whenUnknownArgument() {
        PulsarMCPCliOptions.parseArgs(new String[]{"--unknown", "value"});
    }

    @Test
    public void parseArgs_shouldHandleCaseInsensitiveTransport() {
        String[] args = {"--transport", "HTTP"};
        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);

        org.testng.Assert.assertEquals(options.getTransport(), PulsarMCPCliOptions.TransportType.HTTP);
    }

    @Test
    public void parseArgs_shouldHandleMixedCaseTransport() {
        String[] args = {"--transport", "Stdio"};
        PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);

        org.testng.Assert.assertEquals(options.getTransport(), PulsarMCPCliOptions.TransportType.STDIO);
    }

    @Test
    public void toString_shouldReturnCorrectFormat() throws Exception {
        PulsarMCPCliOptions options = new PulsarMCPCliOptions();

        java.lang.reflect.Field transportField = PulsarMCPCliOptions.class.getDeclaredField("transport");
        transportField.setAccessible(true);
        transportField.set(options, PulsarMCPCliOptions.TransportType.HTTP);

        java.lang.reflect.Field portField = PulsarMCPCliOptions.class.getDeclaredField("httpPort");
        portField.setAccessible(true);
        portField.set(options, 9999);

        String result = options.toString();
        org.testng.Assert.assertTrue(result.contains("transport=HTTP"));
        org.testng.Assert.assertTrue(result.contains("httpPort=9999"));
    }

    @Test
    public void transportType_fromString_shouldReturnCorrectType() {
        org.testng.Assert.assertEquals(
                PulsarMCPCliOptions.TransportType.fromString("stdio"),
                PulsarMCPCliOptions.TransportType.STDIO
        );
        org.testng.Assert.assertEquals(
                PulsarMCPCliOptions.TransportType.fromString("http"),
                PulsarMCPCliOptions.TransportType.HTTP
        );
        org.testng.Assert.assertEquals(
                PulsarMCPCliOptions.TransportType.fromString("STDIO"),
                PulsarMCPCliOptions.TransportType.STDIO
        );
        org.testng.Assert.assertEquals(
                PulsarMCPCliOptions.TransportType.fromString("HTTP"),
                PulsarMCPCliOptions.TransportType.HTTP
        );
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*invalid is not a valid TransportType.*Valid Options: stdio,http.*"
    )
    public void transportType_fromString_shouldThrowException_whenInvalidType() {
        PulsarMCPCliOptions.TransportType.fromString("invalid");
    }

    @Test
    public void transportType_values_shouldHaveCorrectValues() {
        PulsarMCPCliOptions.TransportType[] values = PulsarMCPCliOptions.TransportType.values();

        org.testng.Assert.assertEquals(values.length, 2);
        org.testng.Assert.assertEquals(values[0], PulsarMCPCliOptions.TransportType.STDIO);
        org.testng.Assert.assertEquals(values[1], PulsarMCPCliOptions.TransportType.HTTP);
    }

    @Test
    public void transportType_getValue_shouldReturnCorrectValue() {
        org.testng.Assert.assertEquals(PulsarMCPCliOptions.TransportType.STDIO.getValue(), "stdio");
        org.testng.Assert.assertEquals(PulsarMCPCliOptions.TransportType.HTTP.getValue(), "http");
    }

    @Test
    public void transportType_getDescription_shouldReturnCorrectDescription() {
        org.testng.Assert.assertEquals(
                PulsarMCPCliOptions.TransportType.STDIO.getDescription(),
                "Standard input/output (Claude Desktop)"
        );
        org.testng.Assert.assertEquals(
                PulsarMCPCliOptions.TransportType.HTTP.getDescription(),
                "HTTP Streaming Events (Web application)"
        );
    }
}
