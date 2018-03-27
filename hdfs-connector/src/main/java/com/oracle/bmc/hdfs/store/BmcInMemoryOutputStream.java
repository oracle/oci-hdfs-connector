/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.util.Progressable;

import com.oracle.bmc.hdfs.util.BiFunction;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadRequest;
import com.oracle.bmc.util.StreamUtils;

/**
 * Output stream implementation that backs all writes to an in-memory buffer.
 */
public class BmcInMemoryOutputStream extends BmcOutputStream {
    private final int bufferSizeInBytes;

    private ByteArrayOutputStream baos;

    public BmcInMemoryOutputStream(
            final UploadManager uploadManager,
            final Progressable progress,
            final int bufferSizeInBytes,
            final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn) {
        super(uploadManager, progress, requestBuilderFn);
        this.bufferSizeInBytes = bufferSizeInBytes;
    }

    @Override
    protected OutputStream createOutputBufferStream() {
        this.baos = new ByteArrayOutputStream(this.bufferSizeInBytes);
        return this.baos;
    }

    @Override
    protected long getInputStreamLengthInBytes() throws IOException {
        return this.baos.size();
    }

    @Override
    protected InputStream getInputStreamFromBufferedStream() {
        return StreamUtils.createByteArrayInputStream(this.baos.toByteArray());
    }
}
