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
package org.apache.pulsar.admin.mcp;

import org.apache.pulsar.admin.mcp.config.PulsarMCPCliOptions;
import org.apache.pulsar.admin.mcp.transport.TransportLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    try {
      PulsarMCPCliOptions options = PulsarMCPCliOptions.parseArgs(args);
      logger.info("Starting Pulsar Admin MCP Server with options: {}", options);
      TransportLauncher.start(options);
    } catch (Exception e) {
      logger.error("Fatal error starting Pulsar Admin MCP Server: {}", e.getMessage(), e);
      System.err.println("Fatal error: " + e.getMessage());
      System.exit(-1);
    }
  }
}
