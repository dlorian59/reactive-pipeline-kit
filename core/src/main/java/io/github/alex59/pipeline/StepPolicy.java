package io.github.alex59.pipeline;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Reliability policy for a single step. Timeout applies to each attempt,
 * excluding the delay between retries. Retry count excludes the first attempt.
 * @param timeout positive attempt timeout
 * @param maxRetries maximum additional attempts
 * @param retryDelay fixed nonnegative delay before another attempt
 * @param retryOn predicate deciding which errors may be retried
 */
public record StepPolicy(Duration timeout, int maxRetries, Duration retryDelay,
                         Predicate<Throwable> retryOn) {
    /**
     * Validates duration and count boundaries.
     * @param timeout positive attempt timeout
     * @param maxRetries nonnegative additional attempt count
     * @param retryDelay nonnegative delay between attempts
     * @param retryOn retry eligibility predicate
     */
    public StepPolicy {
        Objects.requireNonNull(timeout, "timeout");
        Objects.requireNonNull(retryDelay, "retryDelay");
        Objects.requireNonNull(retryOn, "retryOn");
        if (timeout.isZero() || timeout.isNegative()) throw new IllegalArgumentException("timeout must be positive");
        if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be nonnegative");
        if (retryDelay.isNegative()) throw new IllegalArgumentException("retryDelay must be nonnegative");
    }

    /**
     * Creates the default policy.
     * @return a 30-second attempt timeout with retries disabled
     */
    public static StepPolicy defaults() {
        return once(Duration.ofSeconds(30));
    }

    /**
     * Bounded profile for read-only remote calls: three seconds per attempt,
     * two additional attempts and 100 milliseconds between attempts. Only direct
     * connection and timeout errors qualify (ConnectException, SocketTimeoutException,
     * HttpTimeoutException, TimeoutException). HTTP statuses are not
     * classified, and wrapped transport exceptions require the predicate overload.
     * Never apply automatically to writes or other non-idempotent operations.
     * @return immutable read-only call policy
     */
    public static StepPolicy readOnlyRemoteCall() {
        return readOnlyRemoteCall(error -> error instanceof java.net.ConnectException
                || error instanceof java.net.SocketTimeoutException
                || error instanceof java.net.http.HttpTimeoutException
                || error instanceof java.util.concurrent.TimeoutException);
    }

    /**
     * Uses the bounded read-only profile with application-owned error classification.
     * @param transientFailure predicate for selected transient errors, not auth or validation failures
     * @return policy with three-second attempts and two delayed retries
     */
    public static StepPolicy readOnlyRemoteCall(Predicate<Throwable> transientFailure) {
        return new StepPolicy(Duration.ofSeconds(3), 2, Duration.ofMillis(100), transientFailure);
    }

    /**
     * Creates a policy with retries disabled.
     * @param timeout attempt timeout
     * @return immutable policy
     */
    public static StepPolicy once(Duration timeout) {
        return new StepPolicy(timeout, 0, Duration.ZERO, error -> false);
    }
}
