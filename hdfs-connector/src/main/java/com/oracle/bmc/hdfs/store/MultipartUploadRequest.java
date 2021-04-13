package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.model.CreateMultipartUploadDetails;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.retrier.DefaultRetryCondition;
import com.oracle.bmc.retrier.RetryCondition;
import com.oracle.bmc.retrier.RetryConfiguration;
import com.oracle.bmc.util.internal.Consumer;
import com.oracle.bmc.waiter.ExponentialBackoffDelayStrategy;
import com.oracle.bmc.waiter.MaxAttemptsTerminationStrategy;
import lombok.NonNull;

import javax.ws.rs.client.Invocation;
import java.util.concurrent.ExecutorService;

/**
 * Helper class to encapsulate the needed info to do a Multipart Upload OutputStream
 */
public class MultipartUploadRequest {
    private final CreateMultipartUploadRequest multipartUploadRequest;
    private ObjectStorage objectStorage;
    private ExecutorService executorService;
    private boolean allowOverwrite;
    private RetryConfiguration retryConfiguration;

    /**
     * Wanted to setup some sane defaults if someone attempts to send in null for this
     */
    private static final RetryCondition RETRY_CONDITION = new DefaultRetryCondition() {
        public boolean shouldBeRetried(@NonNull BmcException e) {
            if (e == null) {
                throw new NullPointerException("e is marked non-null but is null");
            } else {
                return super.shouldBeRetried(e) || e.getStatusCode() == -1 || e.getStatusCode() == 409 && "ConcurrentObjectUpdate".equals(e.getServiceCode());
            }
        }
    };
    static final RetryConfiguration RETRY_CONFIGURATION = RetryConfiguration.builder().terminationStrategy(new MaxAttemptsTerminationStrategy(5)).delayStrategy(new ExponentialBackoffDelayStrategy(100L)).retryCondition((exception) -> {
        return RETRY_CONDITION.shouldBeRetried(exception);
    }).build();

    MultipartUploadRequest(@NonNull CreateMultipartUploadRequest multipartUploadRequest, @NonNull ObjectStorage objectStorage, ExecutorService executorService, boolean allowOverwrite, RetryConfiguration retryConfiguration) {
        // we cannot write any data if this object is not set
        if (multipartUploadRequest.getCreateMultipartUploadDetails() == null) {
            throw new NullPointerException("CreateMultipartUploadDetails must be non-null");
        }

        this.multipartUploadRequest = multipartUploadRequest;
        this.objectStorage = objectStorage;
        this.executorService = executorService;
        this.allowOverwrite = allowOverwrite;
        this.retryConfiguration = retryConfiguration == null ? RETRY_CONFIGURATION : retryConfiguration;
    }

    public static MultipartUploadRequest.Builder builder() {
        return new MultipartUploadRequest.Builder();
    }

    public ObjectStorage getObjectStorage() {
        return objectStorage;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public CreateMultipartUploadRequest getMultipartUploadRequest() {
        return multipartUploadRequest;
    }

    public boolean isAllowOverwrite() {
        return allowOverwrite;
    }

    public RetryConfiguration getRetryConfiguration() {
        return retryConfiguration;
    }

    public static class Builder {
        private CreateMultipartUploadRequest multipartUploadRequest;
        private ObjectStorage objectStorage;
        private ExecutorService executorService;
        private boolean allowOverwrite;
        private RetryConfiguration retryConfiguration = RETRY_CONFIGURATION;

        public MultipartUploadRequest.Builder setObjectStorage(ObjectStorage objectStorage) {
            this.objectStorage = objectStorage;
            return this;
        }

        public MultipartUploadRequest.Builder setExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public MultipartUploadRequest.Builder setAllowOverwrite(boolean allowOverwrite) {
            this.allowOverwrite = allowOverwrite;
            return this;
        }

        public MultipartUploadRequest.Builder setRetryConfiguration(RetryConfiguration retryConfiguration) {
            this.retryConfiguration = retryConfiguration;
            return this;
        }

        public MultipartUploadRequest.Builder setMultipartUploadRequest(CreateMultipartUploadRequest multipartUploadRequest) {
            this.multipartUploadRequest = multipartUploadRequest;
            return this;
        }

        public CreateMultipartUploadRequest getMultipartUploadRequest() {
            return multipartUploadRequest;
        }

        public MultipartUploadRequest build() {
            return new MultipartUploadRequest(multipartUploadRequest, objectStorage, executorService, allowOverwrite, retryConfiguration);
        }
    }
}
