package io.github.alex59.pipeline;

import java.util.Objects;

/**
 * Per-subscription record budget and output order. A record may hold arbitrarily
 * many bytes; this budget does not replace payload limits or a global rate limiter.
 * @param concurrency positive maximum of active record subscriptions
 * @param ordering output order, not side-effect order
 */
public record ProcessingOptions(int concurrency, Ordering ordering) {
    /**
     * Validates the finite positive record budget and output order.
     * @param concurrency maximum active records
     * @param ordering output order
     */
    public ProcessingOptions {
        if (concurrency < 1) throw new IllegalArgumentException("concurrency must be positive");
        Objects.requireNonNull(ordering, "ordering");
    }

    /** @return four active records with results emitted in input order */
    public static ProcessingOptions ordered() { return ordered(4); }

    /**
     * @param concurrency maximum active records
     * @return input-ordered results
     */
    public static ProcessingOptions ordered(int concurrency) {
        return new ProcessingOptions(concurrency, Ordering.INPUT);
    }

    /** @return one active record, preserving invocation and output order */
    public static ProcessingOptions sequential() { return ordered(1); }

    /**
     * @param concurrency maximum active records
     * @return results as each record finishes
     */
    public static ProcessingOptions completionOrder(int concurrency) {
        return new ProcessingOptions(concurrency, Ordering.COMPLETION);
    }
}
