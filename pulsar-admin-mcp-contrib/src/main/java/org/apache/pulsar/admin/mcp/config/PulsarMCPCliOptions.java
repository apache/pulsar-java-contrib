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

import lombok.Getter;

@Getter
public class PulsarMCPCliOptions {

    @Getter
    public enum TransportType {

        STDIO("stdio", "Standard input/output"),
        HTTP("http", "HTTP Server-Sent Events");
        private final String value;
        private final String description;

        TransportType(String value, String description) {
            this.value = value; this.description = description;
        }

        public static TransportType fromString(String value) {
            for (TransportType t : values()) {
                if (t.value.equalsIgnoreCase(value)) {
                    return t;
                }
            }
            throw new IllegalArgumentException(
                    value + " is not a valid TransportType. Valid Options: stdio,http");
        }
    }

    private TransportType transport = TransportType.STDIO;
    private int httpPort = 8889;

    public static PulsarMCPCliOptions parseArgs(String[] args) {
        PulsarMCPCliOptions opts = new PulsarMCPCliOptions();

        String envTransport = System.getenv("MCP_TRANSPORT");
        if (envTransport != null && !envTransport.isBlank()) {
            opts.transport = TransportType.fromString(envTransport);
        }

        String envPort = System.getenv("MCP_HTTP_PORT");
        if (envPort != null && !envPort.isBlank()) {
            try {
                opts.httpPort = Integer.parseInt(envPort);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid MCP_HTTP_PORT: " + envPort);
            }
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "-t", "--transport" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Missing value for --transport");
                    }
                    opts.transport = TransportType.fromString(args[++i]);
                }
                case "--port" -> {
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("Missing value for --port");
                    }
                    try {
                        opts.httpPort = Integer.parseInt(args[++i]);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid port number for --port");
                    }
                }
                default -> {
                    throw new IllegalArgumentException("Unknown argument: " + arg);
                }
            }
        }
        return opts;
    }

    @Override
    public String toString() {
        return "PulsarMCPCliOptions{transport=" + transport
                + ",httpPort=" + httpPort + '}';
    }
}

