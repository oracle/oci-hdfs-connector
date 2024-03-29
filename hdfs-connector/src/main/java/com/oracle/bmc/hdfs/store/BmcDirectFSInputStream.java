/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;

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

    @Override
    public int read() throws IOException {
        this.checkNotClosed();
        // Try reading from the current stream
        LOG.debug("{}: Reading single byte", this);
        try {
            return super.read();
        } catch (IOException e) {
            LOG.warn("{}: Read failed, possibly a stale connection. Will close connection and re-attempt. Exception: {}", this, e);
            // If the stream has been idle for a while, then Object Storage LB closes the connection causing an IOException
            // on the client side. Close the current stream and try again.
            FSStreamUtils.closeQuietly(super.getSourceInputStream());
            super.setSourceInputStream(null);
            return super.read();
        }
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        this.checkNotClosed();
        // Try reading from the current stream
        LOG.debug("{}: Attempting to read offset {} length {}", this, off, len);
        // see https://issues.apache.org/jira/browse/HDFS-10277
        if (len == 0) {
            return 0;
        }
        try {
            return super.read(b, off, len);
        } catch (IOException e) {
            LOG.warn("{}: Read failed, possibly a stale connection. Will close connection and re-attempt.", this, e);
            // If the stream has been idle for a while, then Object Storage LB closes the connection causing an IOException
            // on the client side. Close the current stream and try again.
            FSStreamUtils.closeQuietly(super.getSourceInputStream());
            super.setSourceInputStream(null);
            return super.read(b, off, len);
        }
    }
}
