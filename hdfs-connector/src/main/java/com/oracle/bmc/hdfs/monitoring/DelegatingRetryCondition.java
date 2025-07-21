package com.oracle.bmc.hdfs.monitoring;


import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.retrier.RetryCondition;
import com.oracle.bmc.util.internal.Validate;

/**
 * A wrapper around {@link RetryCondition} that gives a callback with information about the decision
 * whether to retry or not.
 */
public abstract class DelegatingRetryCondition implements RetryCondition {

    private final RetryCondition delegate;

    /**
     * Create a new delegating retry condition that delegates to the {@link RetryCondition} provided
     * in <code>delegate</code>, and after that invokes the callback.
     *
     * @param delegate {@link RetryCondition} making the actual decision of whether to retry
     */
    public DelegatingRetryCondition(RetryCondition delegate) {
        this.delegate = Validate.notNull(delegate, "delegate must not be null");
    }

    @Override
    public boolean shouldBeRetried(BmcException exception) {
        boolean shouldBeRetried = this.delegate.shouldBeRetried(exception);
        intercept(exception, shouldBeRetried);
        return shouldBeRetried;
    }

    /**
     * Callback.
     *
     * @param exception the exception that occurred
     * @param shouldBeRetried whether this attempt should be retried
     */
    public abstract void intercept(BmcException exception, boolean shouldBeRetried);
}
