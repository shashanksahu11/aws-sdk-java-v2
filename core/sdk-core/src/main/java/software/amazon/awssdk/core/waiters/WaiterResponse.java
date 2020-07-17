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

import java.util.Optional;
import software.amazon.awssdk.annotations.SdkPublicApi;

/**
 * The response returned from a waiter operation
 * @param <T> the type of the response
 */
@SdkPublicApi
public interface WaiterResponse<T> {

    /**
     * @return the optional response received that has matched with the waiter success condition
     */
    Optional<T> response();

    /**
     * @return the optional exception thrown from the waiter operation that has matched with the waiter success condition
     */
    Optional<Throwable> exception();

    /**
     * @return the number of attempts executed
     */
    int attemptsExecuted();
}

