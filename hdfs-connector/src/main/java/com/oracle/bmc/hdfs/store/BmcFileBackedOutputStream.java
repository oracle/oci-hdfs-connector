/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.store;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.util.Progressable;

import com.oracle.bmc.hdfs.util.BiFunction;
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

    public BmcFileBackedOutputStream(
            final BmcPropertyAccessor propertyAccessor,
            final UploadManager uploadManager,
            final BiFunction<Long, InputStream, UploadRequest> requestBuilderFn) {
        super(uploadManager, requestBuilderFn);
        this.propertyAccessor = propertyAccessor;
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
    protected long getInputStreamLengthInBytes() throws IOException {
        return this.bufferFile.length();
    }

    @Override
    public void close() throws IOException {
        super.close();
        this.bufferFile.delete();
    }

    private File createBufferFile() throws IOException {
        final File dir = new File(this.propertyAccessor.get("hadoop.tmp.dir"));
        if (!dir.exists() && !dir.mkdirs()) {
            throw new IOException("Unable to create temp file: " + dir);
        }
        final File result = File.createTempFile("oci-", ".tmp", dir);
        LOG.debug("Created temp file {}", result.getAbsolutePath());
        result.deleteOnExit();
        return result;
    }
}
