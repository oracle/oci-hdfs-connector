/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.oracle.bmc.hdfs.util.BiFunction;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadRequest;
import com.oracle.bmc.util.StreamUtils;

/**
 * Output stream implementation that backs all writes to an in-memory buffer.
 */
public class BmcInMemoryOutputStream extends BmcOutputStream {
    private final int bufferSizeInBytes;

    private ByteArrayOutputStream baos;
    protected final boolean isNewFlow;
    protected final UploadConfiguration uploadConfiguration;

    public BmcInMemoryOutputStream(
            final UploadManager uploadManager,
            final int bufferSizeInBytes,
            final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn,
            int writeMaxRetires,
            boolean isNewFlow,
            UploadConfiguration uploadConfiguration) {
        super(uploadManager, requestBuilderFn, writeMaxRetires, isNewFlow, uploadConfiguration);
        this.bufferSizeInBytes = bufferSizeInBytes;
        this.isNewFlow = isNewFlow;
        this.uploadConfiguration = uploadConfiguration;
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
