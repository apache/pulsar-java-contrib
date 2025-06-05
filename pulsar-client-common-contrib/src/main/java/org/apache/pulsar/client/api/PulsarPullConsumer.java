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
package org.apache.pulsar.client.api;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.common.ConsumeStats;
import org.apache.pulsar.client.common.PullRequest;
import org.apache.pulsar.client.common.PullResponse;

/**
 * Pull-based consumer interface with enhanced offset management capabilities.
 *
 * <p>Features:</p>
 * <ul>
 *   <li>Precise offset control with partition-aware operations</li>
 *   <li>Thread-safe design for concurrent access</li>
 *   <li>Support for both partitioned and non-partitioned topics</li>
 *   <li>Built-in offset to message ID mapping</li>
 * </ul>
 *
 * @param <T> message payload type
 */
public interface PulsarPullConsumer<T> extends AutoCloseable {

    /**
     * Initializes consumer resources and establishes connections.
     *
     * @throws PulsarClientException if client initialization fails
     */
    void start() throws PulsarClientException;

    /**
     * Pulls messages from the specified partition starting from the given offset.
     *
     * @param request pull request configuration
     * @return immutable list of messages starting from the specified offset
     * @throws IllegalArgumentException for invalid request parameters
     */
    PullResponse<T> pull(PullRequest request);

    /**
     * Acknowledges all messages up to the specified offset (inclusive).
     *
     * @param offset target offset to acknowledge
     * @param partition partition index (use -1 for non-partitioned topics)
     * @throws PulsarClientException for acknowledgment failures
     * @throws IllegalArgumentException for invalid partition index
     */
    void ack(long offset, int partition) throws PulsarClientException;

    /**
     * Finds the latest message offset before or at the specified timestamp.
     *
     * @param partition partition index (use -1 for non-partitioned topics)
     * @param timestamp target timestamp in milliseconds
     * @return corresponding message offset
     * @throws PulsarAdminException for admin operation failures
     * @throws IllegalArgumentException for invalid partition index
     */
    long searchOffset(int partition, long timestamp) throws PulsarAdminException;

    /**
     * Retrieves consumption statistics for the specified partition.
     *
     * @param partition partition index (use -1 for non-partitioned topics)
     * @return current consumption offset
     * @throws PulsarAdminException     for stats retrieval failures
     * @throws IllegalArgumentException for invalid partition index
     */
    ConsumeStats getConsumeStats(int partition) throws PulsarAdminException;

    /**
     * Releases all resources and closes connections gracefully.
     */
    @Override
    void close();
}
