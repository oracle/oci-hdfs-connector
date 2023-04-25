/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;

import java.util.function.Supplier;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.util.StreamUtils;

/**
 * {@link FSInputStream} implementation that backs the entire file into memory (using a byte array).
 */
public class BmcInMemoryFSInputStream extends BmcFSInputStream {

    public BmcInMemoryFSInputStream(
            final ObjectStorage objectStorage,
            final FileStatus status,
            final Supplier<GetObjectRequest.Builder> requestBuilder,
            final Statistics statistics) {
        super(objectStorage, status, requestBuilder, statistics);
    }

    @Override
    protected long doSeek(final long position) throws IOException {
        // array backed, just reset and skip
        // first validate state to ensure we buffered the entire object into memory
        super.validateState(0);
        final InputStream sourceInputStream = super.getSourceInputStream();
        sourceInputStream.reset();
        return sourceInputStream.skip(position);
    }

    @Override
    protected InputStream wrap(final InputStream rawInputStream) throws IOException {
        return StreamUtils.createByteArrayInputStream(IOUtils.toByteArray(rawInputStream));
    }
}
