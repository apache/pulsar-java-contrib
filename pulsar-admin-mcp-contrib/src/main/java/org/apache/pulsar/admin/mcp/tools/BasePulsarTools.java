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
package org.apache.pulsar.admin.mcp.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpSchema;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasePulsarTools {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BasePulsarTools.class);
    protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected final PulsarAdmin pulsarAdmin;

    public BasePulsarTools(PulsarAdmin pulsarAdmin) {
        if (pulsarAdmin == null) {
            throw new IllegalArgumentException("pulsarAdmin cannot be null");
        }
        this.pulsarAdmin = pulsarAdmin;
    }

    protected McpSchema.CallToolResult createSuccessResult(String message, Object data){
        StringBuilder result = new StringBuilder();
        result.append(message).append("\n");

        if (data != null){
            try {
                String jsonData = OBJECT_MAPPER.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(data);
                result.append(jsonData)
                        .append("\n");
            } catch (Exception e) {
                result.append("Result").append(data.toString()).append("\n");
            }
        }

        return new McpSchema.CallToolResult(
                List.of(new McpSchema.TextContent(result.toString())),
                false
        );
    }

    protected McpSchema.CallToolResult createErrorResult(String message){
        String errorText = "Error：" + message;
        return new McpSchema.CallToolResult(
                List.of(new McpSchema.TextContent(errorText)),
                true
        );
    }

    protected McpSchema.CallToolResult createErrorResult(String message, List<String> suggestions){
        StringBuilder result = new StringBuilder();
        result.append(message).append("\n");

        if (suggestions != null && !suggestions.isEmpty()) {
            result.append(suggestions).append("\n");
            suggestions.forEach(s -> result.append(s).append("\n"));
        }
        return new McpSchema.CallToolResult(
                List.of(new McpSchema.TextContent(result.toString())),
                true
        );
    }

    protected String getStringParam(Map<String, Object> map, String key){
        Object value = map.get(key);
        return value == null ? "" : value.toString();
    }

    protected String getRequiredStringParam(Map<String, Object> map, String key) throws IllegalArgumentException{
        String value = getStringParam(map, key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Required parameter '" + key + "' is missing");
        }
        return value.trim();
    }

    protected Integer getIntParam(Map<String, Object> map, String key, Integer defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }

        try {
            if (value instanceof Number) {
               return ((Number) value).intValue();
            } else {
                return Integer.parseInt(value.toString());
            }
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    protected Boolean getBooleanParam(Map<String, Object> map, String key, Boolean defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else {
            return Boolean.parseBoolean(value.toString());
        }
    }

    protected Long getLongParam(Map<String, Object> arguments, String timestamp, Long defaultValue) {
        Object value = arguments.get(timestamp);
        if (value == null) {
            return defaultValue;
        }

        try {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            } else {
                return Long.parseLong(value.toString());
            }
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    protected static McpSchema.Tool createTool (
            String name,
            String description,
            String inputSchema) {
        return McpSchema.Tool.builder()
                .name(name)
                .description(description)
                .inputSchema(inputSchema)
                .build();
    }


    protected String buildFullTopicName(Map<String, Object> arguments) {
        String topicName = getStringParam(arguments, "topic");
        if (topicName != null && !topicName.isBlank()) {
            if (topicName.startsWith("persistent://") || topicName.startsWith("non-persistent://")) {
                return topicName.trim();
            }
        }

        String tenant = (String) arguments.getOrDefault("tenant", "public");
        String namespace = (String) arguments.getOrDefault("namespace", "default");
        Boolean persistent = (Boolean) arguments.getOrDefault("persistent", true);

        String prefix = persistent ? "persistent://" : "non-persistent://";
        return prefix + tenant + "/" + namespace + "/" + topicName;
    }

    protected String resolveNamespace(Map<String, Object> arguments) {
        String tenant = getStringParam(arguments, "tenant");
        String namespace = getStringParam(arguments, "namespace");

        if (namespace != null && namespace.contains("/")) {
            return namespace;
        }

        if (tenant == null) {
            tenant = "public";
        }

        if (namespace == null) {
            namespace = "default";
        }

        return tenant + "/" + namespace;
    }

    protected void addTopicBreakdown(Map<String, Object> result, String fullTopicName) {
        if (fullTopicName.startsWith("persistent://")) {
            fullTopicName = fullTopicName.substring("persistent://".length());
        } else if (fullTopicName.startsWith("non-persistent://")) {
            fullTopicName = fullTopicName.substring("non-persistent://".length());
        }

        String[] parts = fullTopicName.split("/", 3);
        if (parts.length != 3) {
            return;
        }

        result.put("tenant", parts[0]);
        result.put("namespace", parts[1]);
        result.put("topicName", parts[2]);
    }

}
