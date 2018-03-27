/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;

import com.google.common.base.Supplier;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;

/**
 * Direct stream implementation for reading files. Seeking is achieved by closing the stream and recreating using a
 * range request to the new position.
 */
public class BmcDirectFSInputStream extends BmcFSInputStream {

    public BmcDirectFSInputStream(
            final ObjectStorage objectStorage,
            final FileStatus status,
            final Supplier<GetObjectRequest.Builder> requestBuilder,
            final Statistics statistics) {
        super(objectStorage, status, requestBuilder, statistics);
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
