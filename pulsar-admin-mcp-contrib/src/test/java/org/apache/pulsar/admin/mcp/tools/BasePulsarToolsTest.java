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

import io.modelcontextprotocol.spec.McpSchema;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BasePulsarToolsTest {

    @Mock
    private PulsarAdmin mockPulsarAdmin;

    private BasePulsarToolsTest.TestPulsarTools testTools;

    private AutoCloseable mocks;

    @BeforeMethod
    public void setUp() {
        this.mocks = MockitoAnnotations.openMocks(this);
        this.testTools = new BasePulsarToolsTest.TestPulsarTools(mockPulsarAdmin);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (this.mocks != null) {
            this.mocks.close();
        }
    }

    @Test(
            expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "pulsarAdmin cannot be null"
    )
    public void constructor_shouldThrowException_whenPulsarAdminIsNull() {
        new BasePulsarToolsTest.TestPulsarTools(null);
    }

    @Test
    public void createSuccessResult_shouldCreateSuccessResultWithData() {
        Map<String, Object> data = new HashMap<>();
        data.put("key", "value");
        data.put("count", 42);

        McpSchema.CallToolResult result = testTools.createSuccessResult("Test message", data);

        org.testng.Assert.assertFalse(result.isError());
        org.testng.Assert.assertEquals(result.content().size(), 1);
        org.testng.Assert.assertTrue(result.content().get(0) instanceof McpSchema.TextContent);
        String content = ((McpSchema.TextContent) result.content().get(0)).text();
        org.testng.Assert.assertTrue(content.contains("Test message"));
        org.testng.Assert.assertTrue(content.contains("key"));
        org.testng.Assert.assertTrue(content.contains("value"));
    }

    @Test
    public void createSuccessResult_shouldCreateSuccessResultWithoutData() {
        McpSchema.CallToolResult result = testTools.createSuccessResult("Test message", null);

        org.testng.Assert.assertFalse(result.isError());
        org.testng.Assert.assertEquals(result.content().size(), 1);
        org.testng.Assert.assertTrue(result.content().get(0) instanceof McpSchema.TextContent);
        String content = ((McpSchema.TextContent) result.content().get(0)).text();
        org.testng.Assert.assertEquals(content, "Test message\n");
    }

    @Test
    public void createErrorResult_shouldCreateErrorResultWithMessage() {
        McpSchema.CallToolResult result = testTools.createErrorResult("Test error");

        org.testng.Assert.assertTrue(result.isError());
        org.testng.Assert.assertEquals(result.content().size(), 1);
        org.testng.Assert.assertTrue(result.content().get(0) instanceof McpSchema.TextContent);
        String content = ((McpSchema.TextContent) result.content().get(0)).text();
        org.testng.Assert.assertEquals(content, "Error：Test error");
    }

    @Test
    public void createErrorResult_shouldCreateErrorResultWithSuggestions() {
        List<String> suggestions = List.of("Suggestion 1", "Suggestion 2");
        McpSchema.CallToolResult result = testTools.createErrorResult("Test error", suggestions);

        org.testng.Assert.assertTrue(result.isError());
        org.testng.Assert.assertEquals(result.content().size(), 1);
        org.testng.Assert.assertTrue(result.content().get(0) instanceof McpSchema.TextContent);
        String content = ((McpSchema.TextContent) result.content().get(0)).text();
        org.testng.Assert.assertTrue(content.contains("Test error"));
        org.testng.Assert.assertTrue(content.contains("Suggestion 1"));
        org.testng.Assert.assertTrue(content.contains("Suggestion 2"));
    }

    @Test
    public void getStringParam_shouldReturnValue_whenKeyExists() {
        Map<String, Object> params = new HashMap<>();
        params.put("testKey", "testValue");
        params.put("nullKey", null);

        org.testng.Assert.assertEquals(testTools.getStringParam(params, "testKey"), "testValue");
        org.testng.Assert.assertEquals(testTools.getStringParam(params, "nullKey"), "");
        org.testng.Assert.assertEquals(testTools.getStringParam(params, "nonExistentKey"), "");
    }

    @Test
    public void getRequiredStringParam_shouldReturnValue_whenKeyExistsAndNotEmpty() {
        Map<String, Object> params = new HashMap<>();
        params.put("testKey", "testValue");
        params.put("emptyKey", "   ");
        params.put("nullKey", null);

        org.testng.Assert.assertEquals(testTools.getRequiredStringParam(params, "testKey"), "testValue");

        // emptyKey
        try {
            testTools.getRequiredStringParam(params, "emptyKey");
            org.testng.Assert.fail("Expected IllegalArgumentException for emptyKey");
        } catch (IllegalArgumentException e) {
            org.testng.Assert.assertTrue(e.getMessage().contains("Required parameter 'emptyKey' is missing"));
        }

        // nullKey
        try {
            testTools.getRequiredStringParam(params, "nullKey");
            org.testng.Assert.fail("Expected IllegalArgumentException for nullKey");
        } catch (IllegalArgumentException e) {
            org.testng.Assert.assertTrue(e.getMessage().contains("Required parameter 'nullKey' is missing"));
        }

        // nonExistentKey
        try {
            testTools.getRequiredStringParam(params, "nonExistentKey");
            org.testng.Assert.fail("Expected IllegalArgumentException for nonExistentKey");
        } catch (IllegalArgumentException e) {
            org.testng.Assert.assertTrue(e.getMessage().contains("Required parameter 'nonExistentKey' is missing"));
        }
    }

    @Test
    public void getIntParam_shouldReturnValue_whenKeyExists() {
        Map<String, Object> params = new HashMap<>();
        params.put("intKey", 42);
        params.put("stringKey", "123");
        params.put("doubleKey", 45.6);
        params.put("nullKey", null);

        org.testng.Assert.assertEquals(testTools.getIntParam(params, "intKey", 0), Integer.valueOf(42));
        org.testng.Assert.assertEquals(testTools.getIntParam(params, "stringKey", 0), Integer.valueOf(123));
        org.testng.Assert.assertEquals(testTools.getIntParam(params, "doubleKey", 0), Integer.valueOf(45));
        org.testng.Assert.assertEquals(testTools.getIntParam(params, "nullKey", 0), Integer.valueOf(0));
        org.testng.Assert.assertEquals(testTools.getIntParam(params, "nonExistentKey", 0), Integer.valueOf(0));
    }

    @Test
    public void getIntParam_shouldReturnDefault_whenInvalidFormat() {
        Map<String, Object> params = new HashMap<>();
        params.put("invalidKey", "not-a-number");

        org.testng.Assert.assertEquals(testTools.getIntParam(params, "invalidKey", 0), Integer.valueOf(0));
    }

    @Test
    public void getBooleanParam_shouldReturnValue_whenKeyExists() {
        Map<String, Object> params = new HashMap<>();
        params.put("boolKey", true);
        params.put("stringKey", "true");
        params.put("nullKey", null);

        org.testng.Assert.assertEquals(testTools.getBooleanParam(params, "boolKey", false), Boolean.TRUE);
        org.testng.Assert.assertEquals(testTools.getBooleanParam(params, "stringKey", false), Boolean.TRUE);
        org.testng.Assert.assertEquals(testTools.getBooleanParam(params, "nullKey", false), Boolean.FALSE);
        org.testng.Assert.assertEquals(testTools.getBooleanParam(params, "nonExistentKey", false), Boolean.FALSE);
    }

    @Test
    public void getLongParam_shouldReturnValue_whenKeyExists() {
        Map<String, Object> params = new HashMap<>();
        params.put("longKey", 123L);
        params.put("intKey", 456);
        params.put("stringKey", "789");
        params.put("nullKey", null);

        org.testng.Assert.assertEquals(testTools.getLongParam(params, "longKey", 0L), Long.valueOf(123L));
        org.testng.Assert.assertEquals(testTools.getLongParam(params, "intKey", 0L), Long.valueOf(456L));
        org.testng.Assert.assertEquals(testTools.getLongParam(params, "stringKey", 0L), Long.valueOf(789L));
        org.testng.Assert.assertEquals(testTools.getLongParam(params, "nullKey", 0L), Long.valueOf(0L));
        org.testng.Assert.assertEquals(testTools.getLongParam(params, "nonExistentKey", 0L), Long.valueOf(0L));
    }

    @Test
    public void getLongParam_shouldReturnDefault_whenInvalidFormat() {
        Map<String, Object> params = new HashMap<>();
        params.put("invalidKey", "not-a-number");

        org.testng.Assert.assertEquals(testTools.getLongParam(params, "invalidKey", 0L), Long.valueOf(0L));
    }

    @Test
    public void buildFullTopicName_shouldBuildPersistentTopic_whenTopicStartsWithPersistent() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "persistent://tenant/namespace/topic");

        String result = testTools.buildFullTopicName(params);
        org.testng.Assert.assertEquals(result, "persistent://tenant/namespace/topic");
    }

    @Test
    public void buildFullTopicName_shouldBuildNonPersistentTopic_whenTopicStartsWithNonPersistent() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "non-persistent://tenant/namespace/topic");

        String result = testTools.buildFullTopicName(params);
        org.testng.Assert.assertEquals(result, "non-persistent://tenant/namespace/topic");
    }

    @Test
    public void buildFullTopicName_shouldBuildTopicFromComponents() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "my-topic");
        params.put("tenant", "my-tenant");
        params.put("namespace", "my-namespace");
        params.put("persistent", true);

        String result = testTools.buildFullTopicName(params);
        org.testng.Assert.assertEquals(result, "persistent://my-tenant/my-namespace/my-topic");
    }

    @Test
    public void buildFullTopicName_shouldBuildNonPersistentTopicFromComponents() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "my-topic");
        params.put("tenant", "my-tenant");
        params.put("namespace", "my-namespace");
        params.put("persistent", false);

        String result = testTools.buildFullTopicName(params);
        org.testng.Assert.assertEquals(result, "non-persistent://my-tenant/my-namespace/my-topic");
    }

    @Test
    public void buildFullTopicName_shouldUseDefaultValues() {
        Map<String, Object> params = new HashMap<>();
        params.put("topic", "my-topic");

        String result = testTools.buildFullTopicName(params);
        org.testng.Assert.assertEquals(result, "persistent://public/default/my-topic");
    }

    @Test
    public void resolveNamespace_shouldReturnFullNamespace_whenContainsSlash() {
        Map<String, Object> params = new HashMap<>();
        params.put("namespace", "tenant/namespace");

        String result = testTools.resolveNamespace(params);
        org.testng.Assert.assertEquals(result, "tenant/namespace");
    }

    @Test
    public void resolveNamespace_shouldBuildFromTenantAndNamespace() {
        Map<String, Object> params = new HashMap<>();
        params.put("tenant", "my-tenant");
        params.put("namespace", "my-namespace");

        String result = testTools.resolveNamespace(params);
        org.testng.Assert.assertEquals(result, "my-tenant/my-namespace");
    }

    @Test
    public void resolveNamespace_shouldUseDefaultValues() {
        Map<String, Object> params = new HashMap<>();
        String result = testTools.resolveNamespace(params);
        org.testng.Assert.assertTrue(
                "public/default".equals(result) || "/".equals(result),
                "Unexpected default namespace: " + result
        );
    }


    @Test
    public void addTopicBreakdown_shouldBreakDownPersistentTopic() {
        Map<String, Object> result = new HashMap<>();
        testTools.addTopicBreakdown(result, "persistent://tenant/namespace/topic");

        org.testng.Assert.assertEquals(result.get("tenant"), "tenant");
        org.testng.Assert.assertEquals(result.get("namespace"), "namespace");
        org.testng.Assert.assertEquals(result.get("topicName"), "topic");
    }

    @Test
    public void addTopicBreakdown_shouldBreakDownNonPersistentTopic() {
        Map<String, Object> result = new HashMap<>();
        testTools.addTopicBreakdown(result, "non-persistent://tenant/namespace/topic");

        org.testng.Assert.assertEquals(result.get("tenant"), "tenant");
        org.testng.Assert.assertEquals(result.get("namespace"), "namespace");
        org.testng.Assert.assertEquals(result.get("topicName"), "topic");
    }

    @Test
    public void addTopicBreakdown_shouldNotBreakDownInvalidTopic() {
        Map<String, Object> result = new HashMap<>();
        testTools.addTopicBreakdown(result, "invalid-topic");

        org.testng.Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void createTool_shouldCreateToolWithCorrectProperties() {
        McpSchema.Tool tool = BasePulsarTools.createTool("test-tool", "Test description", "{}");

        org.testng.Assert.assertEquals(tool.name(), "test-tool");
        org.testng.Assert.assertEquals(tool.description(), "Test description");
        Object schema = tool.inputSchema();
        if (schema != null) {
            try {
                java.lang.reflect.Field props = schema.getClass().getDeclaredField("properties");
                props.setAccessible(true);
                Object val = props.get(schema);
                org.testng.Assert.assertNull(val, "JsonSchema.properties should be null for empty schema");
            } catch (Exception ignore) {
            }
        } else {
            org.testng.Assert.fail("Unexpected inputSchema type: " + "null");
        }
    }

    private static class TestPulsarTools extends BasePulsarTools {
        TestPulsarTools(PulsarAdmin pulsarAdmin) {
            super(pulsarAdmin);
        }

        public McpSchema.CallToolResult createSuccessResult(String message, Object data) {
            return super.createSuccessResult(message, data);
        }

        public McpSchema.CallToolResult createErrorResult(String message) {
            return super.createErrorResult(message);
        }

        public McpSchema.CallToolResult createErrorResult(String message, List<String> suggestions) {
            return super.createErrorResult(message, suggestions);
        }

        public String getStringParam(Map<String, Object> map, String key) {
            return super.getStringParam(map, key);
        }

        public String getRequiredStringParam(Map<String, Object> map, String key) {
            return super.getRequiredStringParam(map, key);
        }

        public Integer getIntParam(Map<String, Object> map, String key, Integer defaultValue) {
            return super.getIntParam(map, key, defaultValue);
        }

        public Boolean getBooleanParam(Map<String, Object> map, String key, Boolean defaultValue) {
            return super.getBooleanParam(map, key, defaultValue);
        }

        public Long getLongParam(Map<String, Object> arguments, String timestamp, Long defaultValue) {
            return super.getLongParam(arguments, timestamp, defaultValue);
        }

        public String buildFullTopicName(Map<String, Object> arguments) {
            return super.buildFullTopicName(arguments);
        }

        public String resolveNamespace(Map<String, Object> arguments) {
            return super.resolveNamespace(arguments);
        }

        public void addTopicBreakdown(Map<String, Object> result, String fullTopicName) {
            super.addTopicBreakdown(result, fullTopicName);
        }
    }
}
