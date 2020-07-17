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
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.waiters.PollingStrategy;
import software.amazon.awssdk.core.waiters.WaiterAcceptor;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.core.waiters.WaiterState;

/**
 * The waiter handler helper class
 */
@SdkInternalApi
public final class WaiterHandlerHelper<T> {
    private final List<WaiterAcceptor<T>> waiterAcceptors;
    private final PollingStrategy pollingStrategy;

    public WaiterHandlerHelper(List<WaiterAcceptor<T>> waiterAcceptors, PollingStrategy pollingStrategy) {
        this.waiterAcceptors = waiterAcceptors;
        this.pollingStrategy = pollingStrategy;
    }

    /**
     * A convenience method to create a {@link WaiterResponse} with either a response or an exception
     *
     * @param response nullable response
     * @param exception nullable exception
     * @return a {@link WaiterResponse}
     */
    public WaiterResponse<T> createWaiterResponse(T response, Throwable exception, int attempts) {
        WaiterResponse<T> waiterResponse;
        if (exception != null) {
            waiterResponse = DefaultWaiterResponse.<T>builder().exception(exception).attemptsExecuted(attempts).build();
        } else {
            waiterResponse = DefaultWaiterResponse.<T>builder().response(response).attemptsExecuted(attempts).build();
        }
        return waiterResponse;
    }

    /**
     * Iterates over the acceptors list and returns the {@link WaiterState} of the the first acceptor to match the
     * result of the operation if present or empty otherwise.
     *
     * @param response nullable response to match
     * @param exception nullable exception to match
     * @return the optional {@link WaiterState}
     */
    public Optional<WaiterState> nextWaiterStateIfMatched(T response, Throwable exception) {
        Optional<WaiterState> waiterState;
        if (exception != null) {
            waiterState = exceptionMatches(exception);
        } else {
            waiterState = responseMatches(response);
        }
        return waiterState;
    }

    public long computeNextDelayInMills(int attemptNumber) {
        return pollingStrategy.backoffStrategy()
                              .computeDelayBeforeNextRetry(RetryPolicyContext.builder()
                                                                             .retriesAttempted(attemptNumber)
                                                                             .build())
                              .toMillis();
    }

    private Optional<WaiterState> responseMatches(T response) {
        return waiterAcceptors.stream()
                              .filter(acceptor -> acceptor.matches(response))
                              .findFirst()
                              .map(WaiterAcceptor::waiterState);
    }

    private Optional<WaiterState> exceptionMatches(Throwable exception) {
        return waiterAcceptors.stream()
                              .filter(acceptor -> acceptor.matches(exception))
                              .findFirst()
                              .map(WaiterAcceptor::waiterState);
    }
}
