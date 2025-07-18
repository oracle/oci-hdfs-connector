package com.oracle.bmc.hdfs.monitoring;

import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import com.oracle.bmc.hdfs.waiter.ResettingExponentialBackoffStrategy;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.retrier.*;
import com.oracle.bmc.util.internal.Validate;
import com.oracle.bmc.waiter.DelayStrategy;
import com.oracle.bmc.waiter.ExponentialBackoffDelayStrategy;
import com.oracle.bmc.waiter.MaxTimeTerminationStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A wrapper around {@link RetryCondition} that collects information about the decision whether to retry or not.
 * Logs retry metrics for observability.
 */
public class RetryMetricsCollector implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RetryMetricsCollector.class);
    private final RetryConfiguration retryConfiguration;
    private final AtomicInteger attemptCount = new AtomicInteger(0);
    private final AtomicInteger retry503Count = new AtomicInteger(0);
    private final AtomicInteger retry429Count = new AtomicInteger(0);

    private volatile boolean isClosed = false;
    private final String operation;
    private final List<Integer> retriedStatusCodes = new ArrayList<>();

    /**
     * Constructs the RetryMetricsCollector with a custom RetryConfiguration and operation name.
     *
     * @param delegate  RetryConfiguration to wrap
     * @param operation Operation name (e.g., PUT, GET)
     */
    public RetryMetricsCollector(RetryConfiguration delegate, String operation) {
        Validate.notNull(delegate, "RetryConfiguration must not be null");
        this.operation = operation;

        this.retryConfiguration = RetryConfiguration.builder()
                .terminationStrategy(delegate.getTerminationStrategy())
                .delayStrategy(delegate.getDelayStrategy())
                .retryCondition(new MetricsDelegatingRetryCondition(delegate.getRetryCondition()))
                .retryOptions(delegate.getRetryOptions())
                .build();
    }

    /**
     * Constructs the RetryMetricsCollector using retry configs from the given property accessor.
     *
     * @param operation         Operation name (e.g., PUT, GET)
     * @param propertyAccessor  Property accessor for fetching retry configs
     */
    public RetryMetricsCollector(String operation, BmcPropertyAccessor propertyAccessor) {
        this.operation = operation;
        Validate.notNull(propertyAccessor, "propertyAccessor must not be null");

        long timeoutSeconds = propertyAccessor.asLong().get(BmcProperties.RETRY_TIMEOUT_IN_SECONDS);
        long resetThresholdSeconds = propertyAccessor.asLong().get(BmcProperties.RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS);

        DelayStrategy delayStrategy = (resetThresholdSeconds <= 0)
                ? new ExponentialBackoffDelayStrategy(Duration.ofSeconds(timeoutSeconds).toMillis())
                : new ResettingExponentialBackoffStrategy(resetThresholdSeconds);

        this.retryConfiguration = RetryConfiguration.builder()
                .terminationStrategy(MaxTimeTerminationStrategy.ofSeconds(timeoutSeconds))
                .delayStrategy(delayStrategy)
                .retryCondition(new MetricsDelegatingRetryCondition(RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION.getRetryCondition()))
                .build();
    }

    /**
     * Constructs the RetryMetricsCollector using pre-fetched timeout configs.
     *
     * @param operation Operation name (e.g., PUT, GET)
     * @param timeoutSeconds Timeout in seconds for retry termination
     * @param resetThresholdSeconds Threshold in seconds for resetting backoff
     */
    public RetryMetricsCollector(String operation, long timeoutSeconds, long resetThresholdSeconds) {
        this.operation = operation;
        DelayStrategy delayStrategy = (resetThresholdSeconds <= 0)
                ? new ExponentialBackoffDelayStrategy(Duration.ofSeconds(timeoutSeconds).toMillis())
                : new ResettingExponentialBackoffStrategy(resetThresholdSeconds);
        this.retryConfiguration = RetryConfiguration.builder()
                .terminationStrategy(MaxTimeTerminationStrategy.ofSeconds(timeoutSeconds))
                .delayStrategy(delayStrategy)
                .retryCondition(new MetricsDelegatingRetryCondition(
                        RetryConfiguration.SDK_DEFAULT_RETRY_CONFIGURATION.getRetryCondition()))
                .build();
    }

    public RetryConfiguration getRetryConfiguration() {
        return retryConfiguration;
    }

    /**
     * Record an exception.
     *
     * @param exception        the exception that occurred
     * @param shouldBeRetried whether this attempt should be retried
     */
    protected void recordException(BmcException exception, boolean shouldBeRetried) {
        int statusCode = exception.getStatusCode();
        LOG.debug("Attempt {} received HTTP status {}, should be retried? {}",
                attemptCount.get() + 1, statusCode, shouldBeRetried);
        if (shouldBeRetried) {
            attemptCount.incrementAndGet();
            retriedStatusCodes.add(statusCode);

            // Increment counters for 503 and 429 specifically
            if (statusCode == 503) {
                retry503Count.incrementAndGet();
            } else if (statusCode == 429) {
                retry429Count.incrementAndGet();
            }
        }
    }

    public int getAttemptCount() {
        return attemptCount.get();
    }

    public List<Integer> getRetriedStatusCodes() {
        return retriedStatusCodes;
    }

    public int getRetry503Count() {
        return retry503Count.get();
    }

    public int getRetry429Count() {
        return retry429Count.get();
    }

    @Override
    public synchronized void close() {
        if (!isClosed) {
            isClosed = true;

            int count = attemptCount.get();
            int count503 = getRetry503Count();
            int count429 = getRetry429Count();

            if (count > 0 || count503 > 0 || count429 > 0) {
                LOG.debug("Retry Summary - Operation: {}, RetryAttempts: {}, 503Count: {}, 429Count: {}",
                        operation, count, count503, count429);
                LOG.debug("Retried status codes: {}", retriedStatusCodes);
            }
        }
    }

    public class MetricsDelegatingRetryCondition extends DelegatingRetryCondition {
        public MetricsDelegatingRetryCondition(RetryCondition delegate) {
            super(delegate);
        }

        @Override
        public void intercept(BmcException exception, boolean shouldBeRetried) {
            recordException(exception, shouldBeRetried);
        }
    }
}
