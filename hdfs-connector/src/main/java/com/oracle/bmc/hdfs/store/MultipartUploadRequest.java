package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
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

    public static class Builder {
        private String namespaceName;
        private String bucketName;
        private ObjectStorage objectStorage;
        private ExecutorService executorService;
        private UploadConfiguration uploadConfiguration;
        private String objectName;

        protected Builder() {}

        public MultipartUploadRequest.Builder namespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return this;
        }

        public MultipartUploadRequest.Builder bucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public MultipartUploadRequest.Builder objectStorage(ObjectStorage objectStorage) {
            this.objectStorage = objectStorage;
            return this;
        }

        public MultipartUploadRequest.Builder executorService(ExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public MultipartUploadRequest.Builder uploadConfiguration(UploadConfiguration uploadConfiguration) {
            this.uploadConfiguration = uploadConfiguration;
            return this;
        }

        public MultipartUploadRequest.Builder objectName(String objectName) {
            this.objectName = objectName;
            return this;
        }

        public MultipartUploadRequest build() {
            return new MultipartUploadRequest(this.namespaceName,
                    this.bucketName,
                    this.objectName,
                    this.objectStorage,
                    this.executorService,
                    this.uploadConfiguration);
        }
    }
}
