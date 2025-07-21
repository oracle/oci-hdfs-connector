/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;

import com.oracle.bmc.hdfs.monitoring.RetryMetricsCollector;
import com.oracle.bmc.hdfs.util.FSStreamUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;

import java.util.function.Supplier;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;

/**
 * Direct stream implementation for reading files. Seeking is achieved by closing the stream and recreating using a
 * range request to the new position.
 */
@Slf4j
public class BmcDirectFSInputStream extends BmcFSInputStream {

    public BmcDirectFSInputStream(
            final ObjectStorage objectStorage,
            final FileStatus status,
            final Supplier<GetObjectRequest.Builder> requestBuilder,
            final int readMaxRetries,
            final Statistics statistics,
            final RetryMetricsCollector retryMetricsCollector) {
        super(objectStorage, status, requestBuilder, readMaxRetries, statistics, retryMetricsCollector);
    }

    @Override
    protected long doSeek(final long position) throws IOException {
        // close the current stream and let the validation step recreate it at the requested position
        IOUtils.closeQuietly(super.getSourceInputStream());
        super.setSourceInputStream(null);
        super.validateState(position);
        return super.getPos();
    }
}
