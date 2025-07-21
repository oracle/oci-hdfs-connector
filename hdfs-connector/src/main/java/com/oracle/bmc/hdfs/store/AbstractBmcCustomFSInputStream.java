/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.util.function.Supplier;
import com.oracle.bmc.hdfs.monitoring.RetryMetricsCollector;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

/**
 * {@link BmcFSInputStream} abstract class for custom read stream for users to configure
 * their own streams. Since the constructor is invoked using reflection, subclasses are not allowed
 * to modify the constructor
 */
public abstract class AbstractBmcCustomFSInputStream extends BmcFSInputStream {

    protected final BmcPropertyAccessor propertyAccessor;

    public AbstractBmcCustomFSInputStream(
            BmcPropertyAccessor propertyAccessor,
            ObjectStorage objectStorage,
            FileStatus status,
            Supplier<GetObjectRequest.Builder> requestBuilder,
            int readMaxRetries,
            FileSystem.Statistics statistics,
            RetryMetricsCollector retryMetricsCollector) {
        super(objectStorage, status, requestBuilder, readMaxRetries, statistics, retryMetricsCollector);
        this.propertyAccessor = propertyAccessor;
    }
}
