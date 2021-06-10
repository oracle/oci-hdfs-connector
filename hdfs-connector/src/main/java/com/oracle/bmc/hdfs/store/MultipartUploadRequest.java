package com.oracle.bmc.hdfs.store;

import java.util.Objects;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.retrier.DefaultRetryCondition;
import com.oracle.bmc.retrier.RetryCondition;
import com.oracle.bmc.retrier.RetryConfiguration;
import com.oracle.bmc.waiter.ExponentialBackoffDelayStrategy;
import com.oracle.bmc.waiter.MaxAttemptsTerminationStrategy;
import lombok.Getter;
import lombok.NonNull;

/**
 * Helper class to encapsulate the needed info to do a Multipart Upload OutputStream
 */
@Getter
public class MultipartUploadRequest {
    private final CreateMultipartUploadRequest multipartUploadRequest;
    private ObjectStorage objectStorage;
    private boolean allowOverwrite;
    private RetryConfiguration retryConfiguration;

    /**
     * Retry condition that also retries on 409 - ConcurrentObjectUpdate.
     */
    private static final RetryCondition RETRY_CONDITION = new DefaultRetryCondition() {
        public boolean shouldBeRetried(@NonNull BmcException e) {
            if (e == null) {
                // wanted to setup some sane defaults if someone attempts to send in null for the exception
                throw new NullPointerException("e is marked non-null but is null");
            } else {
                return super.shouldBeRetried(e) ||
                        e.getStatusCode() == 409 && "ConcurrentObjectUpdate".equals(e.getServiceCode());
            }
        }
    };
    static final RetryConfiguration RETRY_CONFIGURATION = RetryConfiguration
            .builder()
            .terminationStrategy(new MaxAttemptsTerminationStrategy(5))
            .delayStrategy(new ExponentialBackoffDelayStrategy(100L))
            .retryCondition((exception) -> RETRY_CONDITION.shouldBeRetried(exception))
            .build();

    @lombok.Builder
    MultipartUploadRequest(
            @NonNull CreateMultipartUploadRequest multipartUploadRequest,
            @NonNull ObjectStorage objectStorage,
            boolean allowOverwrite,
            RetryConfiguration retryConfiguration) {
        this.multipartUploadRequest =
                Objects.requireNonNull(multipartUploadRequest, "multipartUploadRequest must be non-null");

        // we cannot write any data if this object is not set
        if (multipartUploadRequest.getCreateMultipartUploadDetails() == null) {
            throw new NullPointerException("CreateMultipartUploadDetails must be non-null");
        }

        this.objectStorage = Objects.requireNonNull(objectStorage, "objectStorage must be non-null");
        this.allowOverwrite = allowOverwrite;
        this.retryConfiguration = retryConfiguration == null ? RETRY_CONFIGURATION : retryConfiguration;
    }
}
