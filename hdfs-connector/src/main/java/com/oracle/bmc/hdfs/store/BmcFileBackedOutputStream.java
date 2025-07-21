/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.oracle.bmc.hdfs.util.BiFunction;
import com.oracle.bmc.objectstorage.transfer.UploadConfiguration;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import com.oracle.bmc.objectstorage.transfer.UploadManager.UploadRequest;
import com.oracle.bmc.util.StreamUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Output stream implementation that backs the data to a temp file on the filesystem.
 * <p>
 * The temp file location is determined by the hadoop configuration property "hadoop.tmp.dir".
 */
@Slf4j
public class BmcFileBackedOutputStream extends BmcOutputStream {
    private final BmcPropertyAccessor propertyAccessor;

    private File bufferFile;
    protected final boolean isNewFlow;
    protected final UploadConfiguration uploadConfiguration;

    public BmcFileBackedOutputStream(
            final BmcPropertyAccessor propertyAccessor,
            final UploadManager uploadManager,
            final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn,
            int writeMaxRetires,
            boolean isNewFlow,
            UploadConfiguration uploadConfiguration) {
        super(uploadManager, requestBuilderFn, writeMaxRetires,isNewFlow, uploadConfiguration);
        this.propertyAccessor = propertyAccessor;
        this.isNewFlow = isNewFlow;
        this.uploadConfiguration = uploadConfiguration;
    }

    @Override
    protected OutputStream createOutputBufferStream() throws IOException {
        this.bufferFile = this.createBufferFile();
        return new BufferedOutputStream(new FileOutputStream(this.bufferFile));
    }

    @Override
    protected InputStream getInputStreamFromBufferedStream() {
        return StreamUtils.toInputStream(this.bufferFile);
    }

    @Override
    protected long getInputStreamLengthInBytes() {
        // bufferFile may be null if temp file creation failed (e.g., disk full)
        return (this.bufferFile != null) ? this.bufferFile.length() : 0L;
    }

    @Override
    public void close() throws IOException {
        super.close();
        // Avoid NullPointerException if bufferFile was never initialized
        if (this.bufferFile != null) {
            this.bufferFile.delete();
        }
    }

    private File createBufferFile() throws IOException {
        final File dir = new File(this.propertyAccessor.get("hadoop.tmp.dir"));
        if (!dir.exists()) {
            dir.mkdirs();
            if (!dir.exists()) {
                throw new IOException("Unable to create temp file: " + dir);
            }
        }
        final File result = File.createTempFile("oci-", ".tmp", dir);
        LOG.debug("Created temp file {}", result.getAbsolutePath());
        result.deleteOnExit();
        return result;
    }
}
