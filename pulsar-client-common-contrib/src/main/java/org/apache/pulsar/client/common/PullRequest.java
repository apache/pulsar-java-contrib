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
package org.apache.pulsar.client.common;

import java.time.Duration;
import lombok.Data;

/** Configuration object for pull requests. */
@Data
public class PullRequest {
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
    private int partition = Constants.PARTITION_NONE_INDEX;
    private int maxMessages = 100;
    private int maxBytes = 10_485_760; // 10MB
    private Duration timeout = Constants.DEFAULT_OPERATION_TIMEOUT;

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
