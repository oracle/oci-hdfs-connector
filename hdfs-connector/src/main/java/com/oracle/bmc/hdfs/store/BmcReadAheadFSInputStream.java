/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import java.io.IOException;
import java.io.InputStream;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;

import java.util.function.Supplier;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;

import java.util.concurrent.ExecutionException;

/**
 * {@link FSInputStream} implementation that reads ahead to cache chunks of
 * data. Like in-memory, but memory usage is limited making it suitable for all
 * files.
 */
@Slf4j
public class BmcReadAheadFSInputStream extends BmcFSInputStream {
    private byte[] data;
    private long filePos = 0;
    private long dataPos = -1;
    private int dataMax = 0;
    private int dataCurOffset = 0;
    private final int ociReadAheadBlockSize;
    private final Cache<String, ParquetFooterInfo> parquetCache;

    public BmcReadAheadFSInputStream(
            final ObjectStorage objectStorage,
            final FileStatus status,
            final Supplier<GetObjectRequest.Builder> requestBuilder,
            final Statistics statistics,
            final int ociReadAheadBlockSize,
            final Cache<String, ParquetFooterInfo> parquetCache) {
        super(objectStorage, status, requestBuilder, statistics);
        this.ociReadAheadBlockSize = ociReadAheadBlockSize;
        LOG.info("ReadAhead block size is " + ociReadAheadBlockSize);
        this.parquetCache = parquetCache;
    }

    public BmcReadAheadFSInputStream(
            final ObjectStorage objectStorage,
            final FileStatus status,
            final Supplier<GetObjectRequest.Builder> requestBuilder,
            final Statistics statistics,
            final int ociReadAheadBlockSize,
            final String parquetCacheString) {
        super(objectStorage, status, requestBuilder, statistics);
        this.ociReadAheadBlockSize = ociReadAheadBlockSize;
        LOG.info("ReadAhead block size is " + ociReadAheadBlockSize);
        this.parquetCache = configureParquetCache(parquetCacheString);
    }

    private Cache<String, ParquetFooterInfo> configureParquetCache(String spec) {
        return CacheBuilder.from(CacheBuilderSpec.parse(spec))
                .removalListener(BmcReadAheadFSInputStream.getParquetCacheRemovalListener())
                .build();
    }

    @Override
    public long getPos() {
        return filePos;
    }

    @Override
    public int read() throws IOException {
        LOG.debug("{}: Reading single byte at position {}", this, filePos);
        if (dataPos == -1) {
            fillBuffer();
        }
        if (dataPos == -1) {
            return -1;
        }
        filePos++;
        int result = Byte.toUnsignedInt(data[dataCurOffset++]);
        if (atEndOfBuffer()) {
            clearBuffer();
        }
        this.statistics.incrementBytesRead(1);
        return result;
    }

    private void clearBuffer() {
        dataPos = -1;
        data = null;
    }

    private boolean atEndOfBuffer() {
        return dataCurOffset == dataMax;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        LOG.debug("{}: Reading {} bytes at position {}", this, length, position);
        seek(position);
        return read(buffer, offset, length);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        LOG.debug("{}: Reading {} bytes at current position {}", this, length, filePos);
        if (dataPos == -1) {
            fillBuffer();
        }
        if (dataPos == -1) {
            return -1; // EOF
        }
        int n = Math.min(length, dataMax - dataCurOffset);
        System.arraycopy(data, dataCurOffset, buffer, offset, n);
        dataCurOffset += n;
        filePos += n;
        if (atEndOfBuffer()) {
            if (n != length) {
                LOG.debug("{}: Short Read; exhausted buffer", this);
            }
            clearBuffer();
        }
        this.statistics.incrementBytesRead(n);
        return n;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        LOG.debug("{}: ReadFully {} bytes from {}", this, position, length);
        seek(position);
        int nBytes = Math.min((int) (status.getLen() - position), length);
        int pos = offset;
        while (nBytes > 0) {
            int n = read(buffer, pos, nBytes);
            if (n == 0) {
                throw new IOException("Read fully unexpected EOF");
            }
            pos += n;
            nBytes -= n;
        }
    }

    @Override
    protected long doSeek(long pos) throws IOException {
        throw new IOException("doSeek not implemented for read-ahead stream");
    }

    @Override
    public void seek(long pos) {
        LOG.debug("{}: Seek to {}", this, pos);
        filePos = pos;
        if (filePos < dataPos) {
            dataPos = -1;
        } else if (dataPos != -1 && filePos > (dataPos + dataMax)) {
            dataPos = -1;
        } else {
            dataCurOffset = (int) (filePos - dataPos);
        }
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    @Override
    public void close() {
        LOG.debug("{}: Closing", this);
        clearBuffer();
    }

    /**
     * Parquet caching.
     */
    public static class ParquetFooterInfo {
        public byte[] footer;
        public byte[] metadata;
        public int metadataLen;
        public long metadataStart;
    }

    private void fillBuffer() throws IOException {
        LOG.debug("{}: Filling buffer at {} length {}", this, filePos, status.getLen());
        long start = filePos;
        long end;
        if (firstRead) {
            firstRead = false;
            end = Math.min(filePos + FIRST_READ_WINDOW_SIZE, status.getLen());
        } else {
            end = Math.min(filePos + ociReadAheadBlockSize, status.getLen());
        }
        if (end == start) {
            return;
        }
        int len = (int) (end - start);
        LOG.debug(
                "{}: Filling requesting {} bytes from {} to {} (size {})",
                this,
                len,
                start,
                end,
                status.getLen());
        Range range = new Range(start, end);
        GetObjectRequest request = requestBuilder.get().range(range).build();
        String key = request.getObjectName();
        if (len == 8) {
            LOG.debug("{}: Detected footer read", this);
            // Parquet footer. Load from cache (side effect: will load info)
            ParquetFooterInfo fi;
            try {
                fi =
                        parquetCache.get(
                                key,
                                () -> {
                                    LOG.debug("Loading parquet cache for {}", key);
                                    GetObjectResponse response = objectStorage.getObject(request);
                                    ParquetFooterInfo ret = new ParquetFooterInfo();
                                    try (final InputStream is = response.getInputStream()) {
                                        ret.footer = new byte[8];
                                        readAllBytes(is, ret.footer);
                                    }
                                    ret.metadataLen =
                                            Byte.toUnsignedInt(ret.footer[3]) << 24
                                                    | Byte.toUnsignedInt(ret.footer[2]) << 16
                                                    | Byte.toUnsignedInt(ret.footer[1]) << 8
                                                    | Byte.toUnsignedInt(ret.footer[0]) << 0;
                                    long metaEnd = status.getLen() - 8;
                                    long metaStart = metaEnd - ret.metadataLen;
                                    ret.metadataStart = metaStart;
                                    Range mdRange = new Range(metaStart, metaEnd);
                                    GetObjectRequest mdRequest =
                                            requestBuilder.get().range(mdRange).build();
                                    GetObjectResponse mdResponse =
                                            objectStorage.getObject(mdRequest);
                                    ret.metadata = new byte[ret.metadataLen];
                                    try (InputStream is = mdResponse.getInputStream()) {
                                        readAllBytes(is, ret.metadata);
                                    }
                                    return ret;
                                });
            } catch (ExecutionException ex) {
                throw new IOException("Error getting file", ex);
            }
            dataPos = start;
            dataMax = len;
            dataCurOffset = 0;
            data = fi.footer;
            return;
        }
        ParquetFooterInfo fi = parquetCache.getIfPresent(key);
        if (fi != null) {
            if (start == fi.metadataStart) {
                LOG.debug("{}: Detected metadata read", this);
                dataPos = start;
                dataMax = fi.metadataLen;
                dataCurOffset = 0;
                data = fi.metadata;
                return;
            }
            // TODO: If we parsed the metadata, we could probably save the column sizes and adjust the range
            // block to get entire column in subsequent reads
        }
        if (fi == null) {
            LOG.debug("{}: Not using parquet semantics", this);
        }
        // Not a parquet file, or column data; just read normally
        GetObjectResponse response = objectStorage.getObject(request);
        data = new byte[len];
        try (InputStream is = response.getInputStream()) {
            readAllBytes(is, data);
        }
        dataPos = filePos;
        dataMax = len;
        dataCurOffset = 0;
        LOG.debug(
                "{}: After filling, dataPos {}, filePos {}, dataMax {}",
                this,
                dataPos,
                filePos,
                dataMax);
    }

    private String reqString;

    public String toString() {
        if (reqString == null) {
            reqString = "ReadAhead Stream for " + requestBuilder.get().build().getObjectName();
        }
        return reqString;
    }

    static void readAllBytes(InputStream is, byte[] b) throws IOException {
        int offset = 0;
        int n = b.length;
        while (n > 0) {
            int i = is.read(b, offset, n);
            if (i <= 0) throw new IOException("Unexpected EOF");
            offset += i;
            n -= i;
        }
    }

    static RemovalListener<String, ParquetFooterInfo> getParquetCacheRemovalListener() {
        return (RemovalNotification<String, ParquetFooterInfo> rn) -> {
            LOG.debug("Removed entry {}, cause {}", rn.getKey(), rn.getCause());
        };
    }
}
