/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * {@link FSInputStream} implementation that reads ahead to cache chunks of
 * data using parallel ranged GET requests. Like in-memory, but memory usage
 * is limited making it suitable for all files.
 */
@Slf4j
public class BmcParallelReadAheadFSInputStream extends BmcFSInputStream {
    private final NavigableMap<Long, CachedRead> cachedData;
    private final ExecutorService executor;
    private long filePos = 0;
    private final int ociReadAheadBlockSize;
    private final int readAheadBlockCount;

    public BmcParallelReadAheadFSInputStream(
            final ObjectStorage objectStorage,
            final FileStatus status,
            final Supplier<GetObjectRequest.Builder> requestBuilder,
            final FileSystem.Statistics statistics,
            ExecutorService executor,
            int ociReadAheadBlockSize,
            int readAheadBlockCount) {
        super(objectStorage, status, requestBuilder, statistics);

        this.executor = executor;
        this.cachedData = new TreeMap<>();
        this.ociReadAheadBlockSize = ociReadAheadBlockSize;
        this.readAheadBlockCount = readAheadBlockCount;
    }

    @Override
    public long getPos() {
        return filePos;
    }

    @Override
    public int read() throws IOException {
        LOG.debug("{}: Reading single byte at position {}", this, filePos);
        CachedRead cachedRead = getCachedRead(filePos);
        if (cachedRead == null) {
            return -1;
        }
        try {
            byte[] data = cachedRead.future.get();
            int result = Byte.toUnsignedInt(data[(int) (filePos - cachedRead.startOffset)]);
            filePos++;
            if (!cachedRead.containsPosition(filePos)) {
                clearCachedRead(cachedRead, filePos);
            }
            return result;
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while reading data at position {}. Retrying...", filePos);
            return read();
        } catch (ExecutionException e) {
            clearCachedRead(cachedRead, filePos);
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Failed to read", e);
        }
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return readAtPosition(position, buffer, offset, length);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        LOG.debug("{}: Attempting to read offset {} length {} from position {}", this, offset, length, filePos);
        CachedRead cachedRead = getCachedRead(filePos);
        if (cachedRead == null) {
            LOG.debug("{}: No cached read found, returning EOF", this);
            return -1;
        }
        try {
            byte[] data = cachedRead.future.get();
            int dataPos = (int) (filePos - cachedRead.startOffset);
            int bytesToRead = Math.min(length, cachedRead.length - dataPos);
            System.arraycopy(data, dataPos, buffer, offset, bytesToRead);
            filePos += bytesToRead;
            this.statistics.incrementBytesRead(bytesToRead);
            if (!cachedRead.containsPosition(filePos)) {
                clearCachedRead(cachedRead, filePos);
            }
            LOG.debug("{}: Read {} bytes", this, bytesToRead);
            return bytesToRead;
        } catch (InterruptedException e) {
            LOG.warn("{}: Read operation interrupted, retrying...", this);
            return read(buffer, offset, length);
        } catch (ExecutionException e) {
            clearCachedRead(cachedRead, filePos);
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Failed to read", e);
        }
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        LOG.debug("{}: ReadFully {} bytes from {}", this, length, position);
        int nBytes = Math.min((int) (status.getLen() - position), length);
        int offsetPosition = offset;
        while (nBytes > 0) {
            int bytesRead = readAtPosition(position, buffer, offsetPosition, nBytes);
            if (bytesRead == 0) {
                throw new IOException("Read fully unexpected EOF");
            }
            position += bytesRead;
            offsetPosition += bytesRead;
            nBytes -= bytesRead;
        }
    }

    /**
     * Reads a specified number of bytes from the input stream at the given absolute position,
     * without changing the current file offset.
     * This method is intended to provide a mechanism for reading bytes from a specific position
     * without affecting the current position in the stream.
     *
     * @param position the absolute position in the input stream from where the data should be read.
     * @param buffer   the buffer into which the data is read.
     * @param offset   the start offset in the buffer at which the data should be written.
     * @param length   the maximum number of bytes to be read from the input stream.
     * @return the total number of bytes read into the buffer, or -1
     *         if there is no more data because the end of the stream has been reached.
     * @throws IOException if an I/O error occurs during the read operation.
     */
    protected int readAtPosition(long position, byte[] buffer, int offset, int length) throws IOException {
        LOG.debug("{}: Attempting to read offset {} length {} at position {}", this, offset, length, position);
        CachedRead cachedRead = getCachedRead(position);
        if (cachedRead == null) {
            return -1;
        }
        try {
            byte[] data = cachedRead.future.get();
            int dataPos = (int) (position - cachedRead.startOffset);
            int bytesToRead = Math.min(length, cachedRead.length - dataPos);
            System.arraycopy(data, dataPos, buffer, offset, bytesToRead);
            this.statistics.incrementBytesRead(bytesToRead);

            // Check if the cache contains the next position, and clear the cache if not.
            long nextPosition = position + bytesToRead;
            if (!cachedRead.containsPosition(nextPosition)) {
                clearCachedRead(cachedRead, nextPosition);
            }
            return bytesToRead;
        } catch (InterruptedException e) {
            LOG.warn("{}: Read operation interrupted, retrying...", this);
            return readAtPosition(position, buffer, offset, length);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
                throw (IOException) cause;
            }
            throw new IOException("Failed to read", e);
        }
    }

    @Override
    protected long doSeek(long position) throws IOException {
        throw new IOException("doSeek not implemented for read-ahead stream");
    }

    @Override
    public void seek(long position) {
        LOG.debug("{}: Seek to {}", this, position);
        filePos = position;
    }

    @Override
    public void close() {
        LOG.debug("{}: Closing", this);
        Iterator<Map.Entry<Long, CachedRead>> cachedEntries = cachedData.entrySet().iterator();
        while (cachedEntries.hasNext()) {
            CachedRead cachedRead = cachedEntries.next().getValue();
            cachedRead.cancel();
            cachedEntries.remove();
        }
    }

    private CachedRead getCachedRead(long pos) {
        Map.Entry<Long, CachedRead> readEntry = cachedData.floorEntry(pos);
        if (readEntry != null && readEntry.getValue().containsPosition(pos)) {
            return readEntry.getValue();
        }
        startNewReads(pos);
        readEntry = cachedData.floorEntry(pos);
        if (readEntry != null && readEntry.getValue().containsPosition(pos)) {
            return readEntry.getValue();
        }
        return null;
    }

    private void clearCachedRead(CachedRead cachedRead, long nextReadPos) {
        boolean wasRemoved = cachedData.remove(cachedRead.startOffset) != null;
        if (wasRemoved) {
            startNewReads(nextReadPos);
        }
    }

    private void startNewReads(long pos) {
        long endPos = Math.min(pos + (ociReadAheadBlockSize * readAheadBlockCount), status.getLen());
        // remove any cached reads that no longer overlap with the current read range
        Iterator<Map.Entry<Long, CachedRead>> entryIterator = cachedData.headMap(pos).entrySet().iterator();
        while (entryIterator.hasNext()) {
            CachedRead cachedRead = entryIterator.next().getValue();
            if (!cachedRead.containsPosition(pos)) {
                cachedRead.cancel();
                entryIterator.remove();
            }
        }
        entryIterator = cachedData.tailMap(endPos).entrySet().iterator();
        while (entryIterator.hasNext()) {
            entryIterator.next().getValue().cancel();
            entryIterator.remove();
        }

        // start reading from the current position to the end position
        long curPos = pos;
        while (curPos < endPos) {
            Map.Entry<Long, CachedRead> entry = cachedData.floorEntry(curPos);
            if (entry != null && entry.getValue().containsPosition(curPos)) {
                curPos = entry.getValue().startOffset + entry.getValue().length;
            } else {
                /* Determine the maximum read length that will result in a block-sized read.
                   Adjust the readLength to maintain block size. If the read request is smaller than the block size,
                   adjust the readLength to the block size to avoid submitting multiple I/O requests for a single block.
                   This improves performance and reduces the likelihood of issues related to partial block reads
                 */
                int readLength;
                if (firstRead) {
                    // Only for first read make the readLength lesser so that we can get as near to an accurate
                    // ttfb for reads as possible.
                    firstRead = false;
                    readLength = (int) Math.min(FIRST_READ_WINDOW_SIZE, endPos - curPos);
                } else {
                    readLength = (int) Math.min(this.ociReadAheadBlockSize, endPos - curPos);
                }
                Map.Entry<Long, CachedRead> nextEntry = cachedData.higherEntry(curPos);
                if (nextEntry != null) {
                    long nextStartPos = nextEntry.getKey();
                    // check if the next cached read overlaps with the current read
                    if (nextStartPos < curPos + ociReadAheadBlockSize) {
                        // adjust the read length to stop at the start of the next cached read
                        readLength = (int) Math.min(readLength, nextStartPos - curPos);
                    }
                }
                // submit a read request with the adjusted read length
                submitReadRequest(curPos, readLength);
                curPos += readLength;
            }
        }
    }

    private void submitReadRequest(final long offset, final int length) {
        Future<byte[]> readFuture = executor.submit(() -> performRead(offset, length));
        CachedRead read = new CachedRead(offset, length, readFuture);
        cachedData.put(offset, read);
    }

    private byte[] performRead(long offset, int length) throws IOException {
        Range range = new Range(offset, offset + length);
        GetObjectRequest request = requestBuilder.get().range(range).build();
        GetObjectResponse response = objectStorage.getObject(request);
        byte[] data = new byte[length];
        try (InputStream is = response.getInputStream()) {
            readAllBytes(is, data);
        }
        return data;
    }

    private String reqString;

    public String toString() {
        if (reqString == null) {
            reqString = "ParallelReadAhead Stream for " + requestBuilder.get().build().getObjectName();
        }
        return reqString;
    }

    @RequiredArgsConstructor
    private static class CachedRead {
        private final long startOffset;
        private final int length;
        private final Future<byte[]> future;

        private boolean containsPosition(long pos) {
            return pos >= startOffset && pos - startOffset < length;
        }

        private void cancel() {
            future.cancel(true);
        }
    }
}