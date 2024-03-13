/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.addons.smartparquet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheBuilderSpec;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.oracle.bmc.hdfs.store.AbstractBmcCustomFSInputStream;
import com.oracle.bmc.hdfs.store.BmcDataStore;
import com.oracle.bmc.hdfs.store.BmcPropertyAccessor;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem.Statistics;

import static org.apache.parquet.format.Util.readFileMetaData;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;

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
public class BmcSmartParquetFSInputStream extends AbstractBmcCustomFSInputStream {
    private long dataPos = -1;
    private int dataMax = 0;
    private int dataCurOffset = 0;
    private final int ociReadAheadBlockSize;
    private final Cache<String, ParquetFooterInfo> parquetCache;
    private boolean closed = false;
    private InputStream wrappedStream;

    public BmcSmartParquetFSInputStream(
            final BmcPropertyAccessor propertyAccessor,
            final ObjectStorage objectStorage,
            final FileStatus status,
            final Supplier<GetObjectRequest.Builder> requestBuilder,
            final Statistics statistics) {
        super(propertyAccessor, objectStorage, status, requestBuilder, statistics);
        this.ociReadAheadBlockSize = BmcDataStore.getReadAheadSizeInBytes(propertyAccessor);
        this.parquetCache =
                configureParquetCache(BmcDataStore.configureParquetCacheString(propertyAccessor));
    }

    private Cache<String, ParquetFooterInfo> configureParquetCache(String spec) {
        return CacheBuilder.from(CacheBuilderSpec.parse(spec))
                .removalListener(BmcSmartParquetFSInputStream.getParquetCacheRemovalListener())
                .build();
    }

    @Override
    public int read() throws IOException {
        this.checkNotClosed();
        LOG.debug("{}: Reading single byte at position {}", this, this.currentPosition);
        if (dataPos == -1) {
            fillBuffer(1);
        }
        if (dataPos == -1) {
            return -1;
        }
        this.currentPosition++;
        dataCurOffset++;
        this.statistics.incrementBytesRead(1L);
        int ret = wrappedStream.read();
        if (dataCurOffset == dataMax) {
            dataPos = -1;
            closeWrapped();
        }
        return ret;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        this.checkNotClosed();
        LOG.debug("{}: Reading {} bytes at position {}", this, length, position);
        seek(position);
        return read(buffer, offset, length);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        LOG.debug("{}: Reading {} bytes at current position {}", this, length, this.currentPosition);
        this.checkNotClosed();
        // see https://issues.apache.org/jira/browse/HDFS-10277
        if (length == 0) {
            return 0;
        }
        if (dataPos == -1) {
            fillBuffer(length);
        }
        if (dataPos == -1) {
            return -1; // EOF
        }
        int len = Math.min(length, dataMax - dataCurOffset);
        int n = wrappedStream.read(buffer, offset, len);
        dataCurOffset += n;
        this.currentPosition += n;
        this.statistics.incrementBytesRead(n);
        if (dataCurOffset == dataMax) {
            if (n != length) {
                LOG.debug("{}: Short Read; exhausted buffer", this);
            }
            dataPos = -1;
            closeWrapped();
        }
        return n;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        this.checkNotClosed();
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
        if (dataPos == -1) {
            this.currentPosition = pos;
            return pos;
        }
        if (pos < this.currentPosition) {
            dataPos = -1;
            this.currentPosition = pos;
            closeWrapped();
        } else if (pos >= (dataPos + dataMax)) {
            dataPos = -1;
            this.currentPosition = pos;
            closeWrapped();
        } else {
            int len = (int) (pos - this.currentPosition);
            byte[] b = new byte[len];
            while (len > 0) {
                int n = read(b, 0, len);
                if (n <= 0) {
                    throw new IOException("EOF on seek");
                }
                len -= n;
            }
        }
        return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
        return false;
    }

    @Override
    public void close() {
        LOG.debug("{}: Closing", this);
        if (closed) {
            return;
        }
        closed = true;
        dataPos = -1;
        closeWrapped();
    }

    private void closeWrapped() {
        try {
            if (wrappedStream != null) {
                byte[] b = new byte[8192];
                while (wrappedStream.read(b) > 0) ; // Read all data so stream can be reused
                wrappedStream.close();
            }
        } catch (IOException ioe) {
            // The underlying stream may be closed but there's no API to tell....
        }
        wrappedStream = null;
    }

    /**
     * Parquet caching.
     */
    public static class ParquetFooterInfo {
        public byte[] footer;
        public byte[] metadata;
        public int metadataLen;
        public long metadataStart;
        public FileMetaData fileMetaData;
    }

    private void fillBuffer(long requested) throws IOException {
        LOG.debug("{}: Filling buffer at {} length {}", this, this.currentPosition, status.getLen());
        long start = this.currentPosition;
        // Read at least blocksize
        long end = this.currentPosition + Math.max(requested, ociReadAheadBlockSize);
        // But don't read past the end
        end = Math.min(end, status.getLen());
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
        Range range = new Range(start, end - 1);
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
                                    ret.fileMetaData =
                                            readFileMetaData(
                                                    new ByteArrayInputStream(ret.metadata), false);
                                    return ret;
                                });
            } catch (ExecutionException ex) {
                throw new IOException("Error getting file", ex);
            }
            dataPos = start;
            dataMax = len;
            dataCurOffset = 0;
            wrappedStream = new ByteArrayInputStream(fi.footer);
            return;
        }
        GetObjectRequest modRequest = request;
        ParquetFooterInfo fi = parquetCache.getIfPresent(key);
        if (fi != null) {
            if (start == fi.metadataStart) {
                LOG.debug("{}: Detected metadata read", this);
                dataPos = start;
                dataMax = fi.metadataLen;
                dataCurOffset = 0;
                wrappedStream = new ByteArrayInputStream(fi.metadata);
                return;
            }
            LOG.debug("{}: Checking metadata start length ", this, len);
            // Adjust the read size for the number of columns
            List<RowGroup> rgl = fi.fileMetaData.getRow_groups();
            long newLength = 0;
            int columns = 0;
            for (RowGroup rg : rgl) {
                List<ColumnChunk> ccl = rg.getColumns();
                for (ColumnChunk cc : ccl) {
                    if (cc.getFile_offset() == start || columns != 0) {
                        long l = cc.getMeta_data().getTotal_compressed_size();
                        newLength = l;
                        break;
                    }
                }
                if (newLength != 0) {
                    break;
                }
            }
            if (newLength != 0) {
                LOG.debug(
                        "{}: Adjusting request based on {} columns; will read size {}",
                        this,
                        columns,
                        newLength);
                end = start + newLength - 1;
                range = new Range(start, end);
                modRequest = requestBuilder.get().range(range).build();
                len = (int) newLength;
            } else LOG.debug("{}: No metadata found; no adjustment", this);
        } else {
            LOG.debug("{}: Not using parquet semantics", this);
        }
        LOG.debug(
                "{}: Filling requesting {} bytes from {} to {} (size {})",
                this,
                len,
                start,
                end,
                status.getLen());
        GetObjectResponse response = objectStorage.getObject(modRequest);
        wrappedStream = response.getInputStream();
        dataPos = this.currentPosition;
        dataMax = len;
        dataCurOffset = 0;
        LOG.debug(
                "{}: After filling, dataPos {}, this.currentPosition {}, dataMax {}",
                this,
                dataPos,
                this.currentPosition,
                dataMax);
    }

    private String reqString;

    public String toString() {
        if (reqString == null) {
            reqString = "ReadAhead Stream for " + requestBuilder.get().build().getObjectName();
        }
        return reqString;
    }

    static RemovalListener<String, ParquetFooterInfo> getParquetCacheRemovalListener() {
        return (RemovalNotification<String, ParquetFooterInfo> rn) -> {
            LOG.debug("Removed entry {}, cause {}", rn.getKey(), rn.getCause());
        };
    }
}
