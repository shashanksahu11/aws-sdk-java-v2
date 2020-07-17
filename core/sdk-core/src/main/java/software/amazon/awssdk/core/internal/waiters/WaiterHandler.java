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
import java.util.function.Supplier;
import software.amazon.awssdk.annotations.NotThreadSafe;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.waiters.PollingStrategy;
import software.amazon.awssdk.core.waiters.WaiterAcceptor;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.core.waiters.WaiterState;
import software.amazon.awssdk.utils.Validate;

/**
 * Handler for sync waiter operations
 * @param <T> the type of the response
 */
@SdkInternalApi
@NotThreadSafe
public final class WaiterHandler<T> {
    private final PollingStrategy pollingStrategy;
    private final WaiterHandlerHelper<T> handlerHelper;
    private int attemptNumber = 0;

    public WaiterHandler(PollingStrategy pollingStrategy,
                         List<WaiterAcceptor<T>> waiterAcceptors) {
        this.pollingStrategy = Validate.paramNotNull(pollingStrategy, "pollingStrategy");
        Validate.paramNotNull(waiterAcceptors, "waiterAcceptors");
        this.handlerHelper = new WaiterHandlerHelper<>(waiterAcceptors, pollingStrategy);
    }

    WaiterResponse<T> execute(Supplier<T> pollingFunction) {
        ++attemptNumber;
        T response;
        try {
            response = pollingFunction.get();
        } catch (Exception exception) {
            return evaluate(pollingFunction, null, exception);
        }

        return evaluate(pollingFunction, response, null);
    }

    private WaiterResponse<T> evaluate(Supplier<T> pollingFunction, T response, Throwable exception) {
        Optional<WaiterState> waiterState = handlerHelper.nextWaiterStateIfMatched(response, exception);

        if (waiterState.isPresent()) {
            WaiterState state = waiterState.get();
            switch (state) {
                case SUCCESS:
                    return handlerHelper.createWaiterResponse(response, exception, attemptNumber);
                case RETRY:
                    return maybeRetry(pollingFunction);
                case FAILURE:
                    throw SdkClientException.create("A waiter acceptor was matched and transitioned the waiter "
                                                    + "to failure state.");
                default:
                    throw new UnsupportedOperationException();
            }
        }

        if (exception != null) {
            throw SdkClientException.create("An exception was thrown and did not match with any waiter acceptor", exception);
        } else {
            // default to retry if there's no acceptor matched for the response
            return maybeRetry(pollingFunction);
        }
    }

    private WaiterResponse<T> maybeRetry(Supplier<T> pollingFunction) {
        if (attemptNumber >= pollingStrategy.maxAttempts()) {
            throw SdkClientException.create("Waiter has exceeded max retry attempts: " + pollingStrategy.maxAttempts());
        }

        try {
            Thread.sleep(handlerHelper.computeNextDelayInMills(attemptNumber));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw SdkClientException.create("The thread got interrupted", e);
        }
        return execute(pollingFunction);
    }
}
