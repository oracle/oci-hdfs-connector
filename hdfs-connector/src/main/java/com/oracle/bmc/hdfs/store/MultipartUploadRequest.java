package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.model.StorageTier;
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
class MultipartUploadRequest {
    private String namespaceName;
    private String bucketName;
    private ObjectStorage objectStorage;
    private ExecutorService executorService;
    private UploadConfiguration uploadConfiguration;
    private String objectName;
    private StorageTier storageTier;
    private boolean allowOverwrite;
    private String opcClientRequestId;
    private Consumer<Invocation.Builder> invocationCallback;
    private RetryConfiguration retryConfiguration;
    private String cacheControl;
    private String contentDisposition;

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

    public MultipartUploadRequest(String namespaceName, String bucketName, ObjectStorage objectStorage, ExecutorService executorService, UploadConfiguration uploadConfiguration, String objectName, StorageTier storageTier, boolean allowOverwrite, String opcClientRequestId, Consumer<Invocation.Builder> invocationCallback, RetryConfiguration retryConfiguration, String cacheControl, String contentDisposition) {
        this.namespaceName = namespaceName;
        this.bucketName = bucketName;
        this.objectStorage = objectStorage;
        this.executorService = executorService;
        this.uploadConfiguration = uploadConfiguration;
        this.objectName = objectName;
        this.storageTier = storageTier;
        this.allowOverwrite = allowOverwrite;
        this.opcClientRequestId = opcClientRequestId;
        this.invocationCallback = invocationCallback;
        this.retryConfiguration = (retryConfiguration == null) ? RETRY_CONFIGURATION : retryConfiguration;
        this.cacheControl = cacheControl;
        this.contentDisposition = contentDisposition;
    }

    public MultipartUploadRequest(String namespaceName,
                                  String bucketName,
                                  String objectName,
                                  ObjectStorage objectStorage,
                                  ExecutorService executorService,
                                  UploadConfiguration uploadConfiguration) {
        this.namespaceName = namespaceName;
        this.bucketName = bucketName;
        this.objectName = objectName;
        this.objectStorage = objectStorage;
        this.executorService = executorService;
        this.uploadConfiguration = uploadConfiguration;
        this.retryConfiguration = RETRY_CONFIGURATION;
    }

    public static MultipartUploadRequest.Builder builder() {
        return new MultipartUploadRequest.Builder();
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getObjectName() {
        return objectName;
    }

    public ObjectStorage getObjectStorage() {
        return objectStorage;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public UploadConfiguration getUploadConfiguration() {
        return uploadConfiguration;
    }

    public StorageTier getStorageTier() {
        return storageTier;
    }

    public boolean isAllowOverwrite() {
        return allowOverwrite;
    }

    public String getOpcClientRequestId() {
        return opcClientRequestId;
    }

    public Consumer<Invocation.Builder> getInvocationCallback() {
        return invocationCallback;
    }

    public RetryConfiguration getRetryConfiguration() {
        return retryConfiguration;
    }

    public String getCacheControl() {
        return cacheControl;
    }

    public String getContentDisposition() {
        return contentDisposition;
    }

    public static class Builder {
        private String namespaceName;
        private String bucketName;
        private ObjectStorage objectStorage;
        private ExecutorService executorService;
        private UploadConfiguration uploadConfiguration;
        private String objectName;
        private StorageTier storageTier;
        private boolean allowOverwrite;
        private String opcClientRequestId;
        private Consumer<Invocation.Builder> invocationCallback;
        private RetryConfiguration retryConfiguration = RETRY_CONFIGURATION;
        private String cacheControl;
        private String contentDisposition;

        public MultipartUploadRequest.Builder setNamespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return this;
        }

        public MultipartUploadRequest.Builder setBucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public MultipartUploadRequest.Builder setObjectStorage(ObjectStorage objectStorage) {
            this.objectStorage = objectStorage;
            return this;
        }

        public MultipartUploadRequest.Builder setExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public MultipartUploadRequest.Builder setUploadConfiguration(UploadConfiguration uploadConfiguration) {
            this.uploadConfiguration = uploadConfiguration;
            return this;
        }

        public MultipartUploadRequest.Builder setObjectName(String objectName) {
            this.objectName = objectName;
            return this;
        }

        public MultipartUploadRequest.Builder setStorageTier(StorageTier storageTier) {
            this.storageTier = storageTier;
            return this;
        }

        public MultipartUploadRequest.Builder setAllowOverwrite(boolean allowOverwrite) {
            this.allowOverwrite = allowOverwrite;
            return this;
        }

        public MultipartUploadRequest.Builder setOpcClientRequestId(String opcClientRequestId) {
            this.opcClientRequestId = opcClientRequestId;
            return this;
        }

        public MultipartUploadRequest.Builder setInvocationCallback(Consumer<Invocation.Builder> invocationCallback) {
            this.invocationCallback = invocationCallback;
            return this;
        }

        public MultipartUploadRequest.Builder setRetryConfiguration(RetryConfiguration retryConfiguration) {
            this.retryConfiguration = retryConfiguration;
            return this;
        }

        public MultipartUploadRequest.Builder setCacheControl(String cacheControl) {
            this.cacheControl = cacheControl;
            return this;
        }

        public MultipartUploadRequest.Builder setContentDisposition(String contentDisposition) {
            this.contentDisposition = contentDisposition;
            return this;
        }

        public MultipartUploadRequest build() {
            return new MultipartUploadRequest(namespaceName, bucketName, objectStorage, executorService, uploadConfiguration, objectName, storageTier, allowOverwrite, opcClientRequestId, invocationCallback, retryConfiguration, cacheControl, contentDisposition);
        }
    }
}
