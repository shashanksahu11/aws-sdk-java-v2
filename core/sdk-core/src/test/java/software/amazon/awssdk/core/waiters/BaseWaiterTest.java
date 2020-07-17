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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.core.retry.FixedTimeBackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.BackoffStrategy;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;

public abstract class BaseWaiterTest {

    static final String SUCCESS_STATE_MESSAGE = "helloworld";
    static final String NON_SUCCESS_STATE_MESSAGE = "other";
    static ScheduledExecutorService executorService;

    @BeforeClass
    public static void setUp() {
        executorService = Executors.newScheduledThreadPool(2);
    }

    @AfterClass
    public static void tearDown() {
        executorService.shutdown();
    }

    @Test
    public void missingPollingStrategy_shouldThrowException() {
        assertThatThrownBy(() ->Waiter.builder(String.class)
              .build()).hasMessageContaining("pollingStrategy");
    }

    @Test
    public void successOnResponse_matchSuccessInFirstAttempt_shouldReturnResponse() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.successOnResponseAcceptor(s -> s.equals(SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();
        WaiterResponse<String> response = successOnResponseWaiterOperation().apply(1, waiter);
        assertThat(response.response()).contains(SUCCESS_STATE_MESSAGE);
        assertThat(response.attemptsExecuted()).isEqualTo(1);
    }

    @Test
    public void successOnResponse_matchError_shouldThrowException() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.errorOnResponseAcceptor(s -> s.equals(NON_SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();

        assertThatThrownBy(() -> successOnResponseWaiterOperation().apply(2, waiter)).hasMessageContaining("transitioned the waiter to failure state");
    }

    @Test
    public void successOnResponse_matchSuccessInSecondAttempt_shouldReturnResponse() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.successOnResponseAcceptor(s -> s.equals(SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();

        WaiterResponse<String> response = successOnResponseWaiterOperation().apply(2, waiter);
        assertThat(response.response()).contains(SUCCESS_STATE_MESSAGE);
        assertThat(response.attemptsExecuted()).isEqualTo(2);
    }

    @Test
    public void successOnResponse_noMatchExceedsMaxAttempts_shouldRetryThreeTimesAndThrowException() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.successOnResponseAcceptor(s -> s.equals(SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();

        assertThatThrownBy(() -> successOnResponseWaiterOperation().apply(4, waiter)).hasMessageContaining("max retry attempts");
    }

    @Test
    public void successOnResponse_multipleMatchingAcceptors_firstTakesPrecedence() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.errorOnResponseAcceptor(s -> s.equals(SUCCESS_STATE_MESSAGE)))
                                      .addAcceptor(WaiterAcceptor.successOnResponseAcceptor(s -> s.equals(SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();

        assertThatThrownBy(() -> successOnResponseWaiterOperation().apply(1, waiter)).hasMessageContaining("transitioned the waiter to failure state");
    }

    @Test
    public void successOnResponse_fixedBackOffStrategy() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(5).backoffStrategy(FixedDelayBackoffStrategy.create(Duration.ofSeconds(1))))
                                      .addAcceptor(WaiterAcceptor.successOnResponseAcceptor(s -> s.equals(SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();

        long start = System.currentTimeMillis();
        WaiterResponse<String> response = successOnResponseWaiterOperation().apply(5, waiter);
        long end = System.currentTimeMillis();
        assertThat((end - start)).isBetween(4000L, 5000L);
        assertThat(response.attemptsExecuted()).isEqualTo(5);
    }

    @Test
    public void successOnException_matchSuccessInFirstAttempt_shouldReturnException() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.successOnExceptionAcceptor(s -> s.getMessage().contains(SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();

        WaiterResponse<String> response = successOnExceptionWaiterOperation().apply(1, waiter);
        assertThat(response.exception().get()).hasMessageContaining(SUCCESS_STATE_MESSAGE);
        assertThat(response.attemptsExecuted()).isEqualTo(1);
    }

    @Test
    public void successOnException_hasRetryAcceptorMatchSuccessInSecondAttempt_shouldReturnException() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.successOnExceptionAcceptor(s -> s.getMessage().contains(SUCCESS_STATE_MESSAGE)))
                                      .addAcceptor(WaiterAcceptor.retryOnExceptionAcceptor(s -> s.getMessage().contains(NON_SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();

        WaiterResponse<String> response = successOnExceptionWaiterOperation().apply(2, waiter);
        assertThat(response.exception().get()).hasMessageContaining(SUCCESS_STATE_MESSAGE);
        assertThat(response.attemptsExecuted()).isEqualTo(2);
    }

    @Test
    public void successOnException_unexpectedExceptionAndNoRetryAcceptor_shouldThrowException() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.successOnExceptionAcceptor(s -> s.getMessage().contains(SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();
        assertThatThrownBy(() -> successOnExceptionWaiterOperation().apply(2, waiter)).hasMessageContaining("did not match");
    }

    @Test
    public void successOnException_matchError_shouldThrowException() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.successOnExceptionAcceptor(s -> s.getMessage().contains(SUCCESS_STATE_MESSAGE)))
                                      .addAcceptor(WaiterAcceptor.errorOnExceptionAcceptor(s -> s.getMessage().contains(NON_SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();
        assertThatThrownBy(() -> successOnExceptionWaiterOperation().apply(2, waiter)).hasMessageContaining("transitioned the waiter to failure state");
    }

    @Test
    public void successOnException_multipleMatchingAcceptors_firstTakesPrecedence() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(3).backoffStrategy(BackoffStrategy.none()))
                                      .addAcceptor(WaiterAcceptor.successOnExceptionAcceptor(s -> s.getMessage().contains(SUCCESS_STATE_MESSAGE)))
                                      .addAcceptor(WaiterAcceptor.errorOnExceptionAcceptor(s -> s.getMessage().contains(SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();

        WaiterResponse<String> response = successOnExceptionWaiterOperation().apply(1, waiter);
        assertThat(response.exception().get()).hasMessageContaining(SUCCESS_STATE_MESSAGE);
        assertThat(response.attemptsExecuted()).isEqualTo(1);
    }

    @Test
    public void successOnException_fixedBackOffStrategy() {
        Waiter<String> waiter = Waiter.builder(String.class)
                                      .pollingStrategy(p -> p.maxAttempts(5).backoffStrategy(FixedDelayBackoffStrategy.create(Duration.ofSeconds(1))))
                                      .addAcceptor(WaiterAcceptor.retryOnExceptionAcceptor(s -> s.getMessage().equals(NON_SUCCESS_STATE_MESSAGE)))
                                      .addAcceptor(WaiterAcceptor.successOnExceptionAcceptor(s -> s.getMessage().equals(SUCCESS_STATE_MESSAGE)))
                                      .scheduledExecutorService(executorService)
                                      .build();

        long start = System.currentTimeMillis();
        WaiterResponse<String> response = successOnExceptionWaiterOperation().apply(5, waiter);
        long end = System.currentTimeMillis();
        assertThat((end - start)).isBetween(4000L, 5000L);
        assertThat(response.attemptsExecuted()).isEqualTo(5);
    }


    public abstract BiFunction<Integer, Waiter<String>, WaiterResponse<String>> successOnResponseWaiterOperation();

    public abstract BiFunction<Integer, Waiter<String>, WaiterResponse<String>> successOnExceptionWaiterOperation();

}
