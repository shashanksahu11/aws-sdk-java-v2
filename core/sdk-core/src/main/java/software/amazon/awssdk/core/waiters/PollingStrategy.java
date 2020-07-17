/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.core.waiters;

import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.utils.Validate;

/**
 * Define the polling strategy for a {@link Waiter} to poll the resource
 */
@SdkPublicApi
public final class PollingStrategy {

    private final int maxAttempts;
    private final BackoffStrategy backoffStrategy;

    public PollingStrategy(Builder builder) {
        this.maxAttempts = Validate.paramNotNull(builder.maxAttempts, "maxAttempts");
        this.backoffStrategy = Validate.paramNotNull(builder.backoffStrategy, "backoffStrategy");
    }

    static Builder builder() {
        return new Builder();
    }

    /**
     * Define the maximum number of attempts to try before transitioning the waiter to a failure state.
     * @return a reference to this object so that method calls can be chained together.
     */
    public int maxAttempts() {
        return maxAttempts;
    }

    /**
     * Define the {@link BackoffStrategy} that computes the delay before the next retry request.
     *
     * @return a reference to this object so that method calls can be chained together.
     */
    public BackoffStrategy backoffStrategy() {
        return backoffStrategy;
    }

    public static final class Builder {
        private BackoffStrategy backoffStrategy;
        private Integer maxAttempts;

        private Builder() {
        }

        /**
         * Define the {@link BackoffStrategy} that computes the delay before the next retry request.
         *
         * @param backoffStrategy The new backoffStrategy value.
         * @return This object for method chaining.
         */
        public Builder backoffStrategy(BackoffStrategy backoffStrategy) {
            this.backoffStrategy = backoffStrategy;
            return this;
        }

        /**
         * Define the maximum number of attempts to try before transitioning the waiter to a failure state.
         *
         * @param maxAttempts The new maxAttempts value.
         * @return This object for method chaining.
         */
        public Builder maxAttempts(Integer maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public PollingStrategy build() {
            return new PollingStrategy(this);
        }
    }
}
