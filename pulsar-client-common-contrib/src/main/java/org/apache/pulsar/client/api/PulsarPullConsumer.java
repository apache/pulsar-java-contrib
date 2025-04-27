package org.apache.pulsar.client.api;

import java.time.Duration;
import java.util.List;
import lombok.Data;
import org.apache.pulsar.client.admin.PulsarAdminException;

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
    int PARTITION_NONE = -1;
    Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofSeconds(30);

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
    List<Message<T>> pull(PullRequest request);

    /**
     * Acknowledges all messages up to the specified offset (inclusive).
     *
     * @param offset target offset to acknowledge
     * @param partition partition index (use {@link #PARTITION_NONE} for non-partitioned topics)
     * @throws PulsarClientException for acknowledgment failures
     * @throws IllegalArgumentException for invalid partition index
     */
    void ack(long offset, int partition) throws PulsarClientException;

    /**
     * Finds the latest message offset before or at the specified timestamp.
     *
     * @param partition partition index (use {@link #PARTITION_NONE} for non-partitioned topics)
     * @param timestamp target timestamp in milliseconds
     * @return corresponding message offset
     * @throws PulsarAdminException for admin operation failures
     * @throws IllegalArgumentException for invalid partition index
     */
    long searchOffset(int partition, long timestamp) throws PulsarAdminException;

    /**
     * Retrieves consumption statistics for the specified partition.
     *
     * @param partition partition index (use {@link #PARTITION_NONE} for non-partitioned topics)
     * @return current consumption offset
     * @throws PulsarAdminException for stats retrieval failures
     * @throws IllegalArgumentException for invalid partition index
     */
    long getConsumeStats(int partition) throws PulsarAdminException;

    /**
     * Releases all resources and closes connections gracefully.
     */
    @Override
    void close();

    /**
     * Configuration object for pull requests.
     */
    @Data
    class PullRequest {
        private final long offset;
        private final int partition;
        private final int maxMessages;
        private final int maxBytes;
        private final Duration timeout;

        private PullRequest(Builder builder) {
            this.offset = builder.offset;
            this.partition = builder.partition;
            this.maxMessages = builder.maxMessages;
            this.maxBytes = builder.maxBytes;
            this.timeout = builder.timeout;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private long offset = -1L;
            private int partition = PARTITION_NONE;
            private int maxMessages = 100;
            private int maxBytes = 10_485_760; // 10MB
            private Duration timeout = DEFAULT_OPERATION_TIMEOUT;

            public Builder offset(long offset) {
                this.offset = offset;
                return this;
            }

            public Builder partition(int partition) {
                this.partition = partition;
                return this;
            }

            public Builder maxMessages(int maxMessages) {
                this.maxMessages = maxMessages;
                return this;
            }

            public Builder maxBytes(int maxBytes) {
                this.maxBytes = maxBytes;
                return this;
            }

            public Builder timeout(Duration timeout) {
                this.timeout = timeout;
                return this;
            }

            public PullRequest build() {
                validate();
                return new PullRequest(this);
            }

            private void validate() {
                if (maxMessages <= 0 || maxBytes <= 0) {
                    throw new IllegalArgumentException("Max messages/bytes must be positive");
                }
            }
        }
    }
}
