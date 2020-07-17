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

package software.amazon.awssdk.core.internal.waiters;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.waiters.PollingStrategy;
import software.amazon.awssdk.core.waiters.WaiterAcceptor;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.core.waiters.WaiterState;
import software.amazon.awssdk.utils.Logger;
import software.amazon.awssdk.utils.Validate;

/**
 * Handler for async waiter operations
 * @param <T> the type of the response
 */
@SdkInternalApi
@NotThreadSafe
public final class AsyncWaiterHandler<T> {
    private static final Logger log = Logger.loggerFor(AsyncWaiterHandler.class);
    private final PollingStrategy pollingStrategy;
    private final ScheduledExecutorService executorService;
    private final WaiterHandlerHelper<T> handlerHelper;
    private int attemptNumber = 0;

    public AsyncWaiterHandler(PollingStrategy pollingStrategy,
                              List<WaiterAcceptor<T>> waiterAcceptors,
                              ScheduledExecutorService executorService) {
        this.pollingStrategy = Validate.paramNotNull(pollingStrategy, "pollingStrategy");
        Validate.paramNotNull(waiterAcceptors, "waiterAcceptors");
        this.executorService = Validate.paramNotNull(executorService, "executorService");
        this.handlerHelper = new WaiterHandlerHelper<>(waiterAcceptors, pollingStrategy);
    }

    /**
     * Execute the provided async polling function
     */
    CompletableFuture<WaiterResponse<T>> execute(Supplier<CompletableFuture<T>> asyncPollingFunction) {
        log.info(() -> "starting to execute");
        CompletableFuture<WaiterResponse<T>> future = new CompletableFuture<>();
        doExecute(asyncPollingFunction, future);
        return future;
    }

    private void doExecute(Supplier<CompletableFuture<T>> asyncPollingFunction, CompletableFuture<WaiterResponse<T>> future) {
        ++attemptNumber;
        log.info(() -> "runOnce" + attemptNumber);
        runAsyncPollingFunction(asyncPollingFunction, future);
    }

    private void runAsyncPollingFunction(Supplier<CompletableFuture<T>> asyncPollingFunction,
                                         CompletableFuture<WaiterResponse<T>> future) {
        asyncPollingFunction.get().whenComplete((response, exception) -> {
            Optional<WaiterState> waiterState = handlerHelper.nextWaiterStateIfMatched(response, exception);

            if (waiterState.isPresent()) {
                log.info(() -> "waiter state " + waiterState.get());
                WaiterState state = waiterState.get();
                switch (state) {
                    case SUCCESS:
                        future.complete(handlerHelper.createWaiterResponse(response, exception, attemptNumber));
                        break;
                    case RETRY:
                        maybeRetry(asyncPollingFunction, future);
                        break;
                    case FAILURE:
                        future.completeExceptionally(SdkClientException.create("A waiter acceptor was matched and transitioned "
                                                                               + "the waiter to failure state"));
                        break;
                    default:
                        future.completeExceptionally(new UnsupportedOperationException());
                }
            } else {
                if (exception != null) {
                    future.completeExceptionally(SdkClientException.create("An exception was thrown and did not match any "
                                                                           + "waiter acceptors"));
                } else {
                    // default to retry if there's no acceptor matched for the response
                    maybeRetry(asyncPollingFunction, future);
                }
            }
        });
    }

    private void maybeRetry(Supplier<CompletableFuture<T>> asyncPollingFunction,
                            CompletableFuture<WaiterResponse<T>> future) {
        if (attemptNumber >= pollingStrategy.maxAttempts()) {
            future.completeExceptionally(SdkClientException.create("The waiter has exceeded max retry attempts: " +
                                                                   pollingStrategy.maxAttempts()));
            return;
        }
        executorService.schedule(
            () -> doExecute(asyncPollingFunction, future), handlerHelper.computeNextDelayInMills(attemptNumber),
            TimeUnit.MILLISECONDS
        );
    }
}
