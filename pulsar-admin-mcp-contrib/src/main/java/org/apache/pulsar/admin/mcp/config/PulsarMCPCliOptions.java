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

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class PulsarMCPCliOptions {

    @Getter
    public enum TransportType {
        STDIO("stdio", "Standard input/ouput (Claude Desktop)"),
        HTTP("http", "HTTP Server-Sent Events (Web application)");

        private final String value;
        private final String description;

        TransportType(String value, String description) {
            this.value = value;
            this.description = description;
        }

        public static TransportType fromString(String value) {
            for (TransportType type : TransportType.values()) {
                if (type.value.equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException(value
                    + " is not a valid TransportType"
                    + ".Valid Options: "
                    + Arrays.stream(values())
                    .map(TransportType::getValue)
                    .collect(Collectors.joining(",")));
        }

    }

    private TransportType transport = TransportType.STDIO;
    private boolean help =  false;
    private boolean listTools = false;

    private String adminUrl = "http://localhost:8080";
    private String serviceUrl = "pulsar://localhost:6650";
    private int httpPort = 8889;

    private String configFile = null;
    private String allowTools = null;
    private String blockTools = null;

    public static PulsarMCPCliOptions parseArgs(String[] args) {
        PulsarMCPCliOptions options = new PulsarMCPCliOptions();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                case "-h", "--help" ->
                        options.help = true;

                case "--list-tools" ->
                        options.listTools = true;

                case "-t", "--transport" -> {
                    if (i + 1 < args.length) {
                        options.transport = TransportType.fromString(args[++i]);
                    } else {
                        throw new IllegalArgumentException("Missing value for --transport");
                    }
                }

                case "--admin-url" -> {
                    if (i + 1 < args.length) {
                        options.adminUrl = args[++i];
                    } else {
                        throw new IllegalArgumentException("Missing value for --admin-url");
                    }
                }

                case "--service-url" -> {
                    if (i + 1 < args.length) {
                        options.serviceUrl = args[++i];
                    } else {
                        throw new IllegalArgumentException("Missing value for --service-url");
                    }
                }

                case "--port" -> {
                    if (i + 1 < args.length) {
                        try {
                            options.httpPort = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("Invalid port number for --port");
                        }
                    } else {
                        throw new IllegalArgumentException("Missing value for --port");
                    }
                }


                case "--config-file" -> options.configFile = args[++i];

                case "--allow-tools" -> options.allowTools = args[++i];

                case "--block-tools" -> options.blockTools = args[++i];

                default -> {
                    try {
                        options.transport = TransportType.fromString(args[++i]);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("Unknown argument: " + arg);
                    }
                }
            }
        }

        if (options.configFile != null) {
            options.loadConfigFile();
        }

        options.applyEnvironmentOverrides();
        return options;
    }

    private void applyEnvironmentOverrides() {
        adminUrl = getEnvOrDefault("PULSAR_ADMIN_URL", adminUrl);
        serviceUrl = getEnvOrDefault("PULSAR_SERVICE_URL", serviceUrl);

        String envTransport = System.getenv("MCP_TRANSPORT");
        if (envTransport != null) {
            transport = TransportType.fromString(envTransport);
        }

        String envPort = System.getenv("MCP_HTTP_PORT");
        if (envPort != null) {
            httpPort = Integer.parseInt(envPort);
        }

    }

    private String getEnvOrDefault(String pulsarAdminUrl, String defaultValue) {
        String value = System.getenv(pulsarAdminUrl);
        return value == null ? defaultValue : value;
    }

    private void loadConfigFile() {
        Path configPath = Paths.get(configFile);
        if (!Files.exists(configPath)) {
            throw new IllegalArgumentException(configFile + " does not exist");
        }

        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configPath.toFile())){
            props.load(fis);
            applyProperties(props);
        } catch (Exception e) {
            throw new IllegalArgumentException(configFile + " could not be loaded" + e);
        }
    }

    private void applyProperties(Properties props) {
        adminUrl = props.getProperty("pulsar.admin.url", adminUrl);
        serviceUrl = props.getProperty("pulsar.serviceUrl", serviceUrl);

        String portStr =  props.getProperty("mcp.http.port");
        if (portStr != null) {
            httpPort = Integer.parseInt(portStr);
        }

        String transportStr = props.getProperty("mcp.transport");
        if (transportStr != null) {
            transport = TransportType.fromString(transportStr);
        }

        allowTools = props.getProperty("mcp.allow.tools", allowTools);
        blockTools = props.getProperty("mcp.block.tools", blockTools);

    }

    private Set<String> parseToolList(String toolList) {
        return Arrays.stream(toolList.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
    }

    public Set<String> getFilteredTools(Set<String> allTools) {
        Set<String> filtered = Set.copyOf(allTools);

        if (allowTools != null && !allowTools.trim().isEmpty()) {
            Set<String> allowed = parseToolList(allowTools);
            filtered = filtered.stream()
                    .filter(allowed::contains)
                    .collect(Collectors.toSet());
        }

        if (blockTools != null && !blockTools.trim().isEmpty()) {
            Set<String> blocked = parseToolList(blockTools);
            filtered = filtered.stream()
                    .filter(tool -> !blocked.contains(tool))
                    .collect(Collectors.toSet());
        }

        return filtered;
    }

    public static void printUsage() {
    }

    public static void printAvailableTools(){
    }

    @Override
    public String toString(){
        return "PulsarMCPCliOptions{"
                + "transport="
                + transport
                + ",adminUrl='"
                + adminUrl
                + '\''
                + ",serviceUrl='"
                + serviceUrl
                + '\''
                + "httpPort="
                + httpPort
                + ",configFile='"
                + configFile
                + '\''
                + ",allowTools='"
                + allowTools
                + '\''
                + ",blockTools='"
                + blockTools
                + '\''
                + '}';
    }
}
