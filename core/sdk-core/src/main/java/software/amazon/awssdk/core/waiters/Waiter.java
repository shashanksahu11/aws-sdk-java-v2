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


import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.core.internal.waiters.DefaultWaiter;

/**
 * Waiter utility class that waits for a resource to transition to the desired state.
 *
 * @param <T> the type of the resource returned from the polling function
 */
@SdkPublicApi
public interface Waiter<T> {

    /**
     * Runs the provided polling function. It completes when the resource enters into a desired state or
     * it is determined that the resource will never enter into the desired state.
     *
     * @param asyncPollingFunction the polling function to trigger
     * @return A CompletableFuture containing the result of the DescribeTable operation returned by the service. It completes
     * successfully when the resource enters into a desired state or it completes exceptionally when it is determined that the
     * resource will never enter into the desired state.
     */
    CompletableFuture<WaiterResponse<T>> runAsync(Supplier<CompletableFuture<T>> asyncPollingFunction);

    /**
     * It returns when the resource enters into a desired state or
     * it is determined that the resource will never enter into the desired state.
     *
     * @param pollingFunction Represents the input of a <code>DescribeTable</code> operation.
     * @return the response
     */
    WaiterResponse<T> run(Supplier<T> pollingFunction);

    /**
     * Creates a newly initialized builder for the waiter object.
     *
     * @param responseClass the response class
     * @param <T> the type of the response
     * @return a Waiter builder
     */
    static <T> Builder<T> builder(Class<? extends T> responseClass) {
        return DefaultWaiter.builder();
    }

    /**
     * The Waiter Builder
     * @param <T> the type of the resource
     */
    interface Builder<T> {
        /**
         * Defines a list of {@link WaiterAcceptor}s to check whether an expected state has met after executing an operation.
         *
         * <p>
         * The SDK will iterate over the acceptors list and the first acceptor to match the result of the operation transitions
         * the waiter to the state specified in the acceptor.
         *
         * <p>
         * This completely overrides any WaiterAcceptor currently configured in the builder via
         * {@link #addAcceptor(WaiterAcceptor)}
         *
         * @param waiterAcceptors the waiter acceptors
         * @return a reference to this object so that method calls can be chained together.
         */
        Builder<T> acceptors(List<WaiterAcceptor<T>> waiterAcceptors);

        /**
         * Adds a {@link WaiterAcceptor} to the end of the ordered waiterAcceptors list.
         *
         * <p>
         * The SDK will iterate over the acceptors list and the first acceptor to match the result of the operation transitions
         * the waiter to the state specified in the acceptor.
         *
         * @param waiterAcceptors the waiter acceptors
         * @return a reference to this object so that method calls can be chained together.
         */
        Builder<T> addAcceptor(WaiterAcceptor<T> waiterAcceptors);

        /**
         * Defines a {@link PollingStrategy} to use when polling a resource
         *
         * @param pollingStrategy the polling strategy to use
         * @return a reference to this object so that method calls can be chained together.
         */
        Builder<T> pollingStrategy(PollingStrategy pollingStrategy);

        /**
         * Defines a {@link PollingStrategy} to use when polling a resource
         *
         * @param pollingStrategy the polling strategy to use
         * @return a reference to this object so that method calls can be chained together.
         */
        default Builder<T> pollingStrategy(Consumer<PollingStrategy.Builder> pollingStrategy) {
            PollingStrategy.Builder builder = PollingStrategy.builder();
            pollingStrategy.accept(builder);
            return pollingStrategy(builder.build());
        }

        /**
         * Defines the {@link ScheduledExecutorService} used to schedule async polling attempts
         * Only required if you are calling {@link Waiter#runAsync(Supplier)}
         *
         * @param scheduledExecutorService the schedule executor service
         * @return a reference to this object so that method calls can be chained together.
         */
        Builder<T> scheduledExecutorService(ScheduledExecutorService scheduledExecutorService);

        /**
         * An immutable object that is created from the properties that have been set on the builder.
         * @return a reference to this object so that method calls can be chained together.
         */
        Waiter<T> build();
    }
}