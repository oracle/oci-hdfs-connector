package com.oracle.bmc.hdfs.caching.internal;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.oracle.bmc.util.StreamUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is a multiplexer, handing out client {@link java.io.InputStream} instances, and any of them will
 * read the same data coming from another source {@link java.io.InputStream}, from the beginning of that source input
 * stream.
 *
 * The data will be buffered in a {@link java.io.File} on disk. Whenver a client input stream reads farther than any
 * other client input stream has read, more data will be read from the source input stream and put into the buffer.
 *
 * Any client input stream can be closed, but that does not close the source input stream.
 */
@Slf4j
public class BufferedInputStreamMultiplexer {
    private final InputStream source;
    private final Buffer buffer;

    /**
     * Create a buffered input stream multiplexer, using the provided input stream as source for the data,
     * and buffering the data in the provided buffer.
     * @param source source input stream
     * @param buffer buffer to use
     */
    public BufferedInputStreamMultiplexer(InputStream source, Buffer buffer) {
        this.source = new BufferedInputStream(source);
        this.buffer = buffer;
        buffer.setMultiplexer(this);
    }

    /**
     * Read a single byte from the source.
     *
     * If the end is reached or an {@link IOException} is thrown, the input stream will be closed.
     *
     * @return next byte, or -1 if end of stream
     * @throws IOException if there is a problem reading the input stream
     */
    private synchronized int readFromSource() throws IOException {
        try {
            int read = source.read();
            if (read == -1) {
                buffer.closeSource();
                StreamUtils.closeQuietly(source);
            } else {
                buffer.write(read);
            }
            return read;
        } catch(IOException ioe) {
            buffer.closeSource();
            StreamUtils.closeQuietly(source);
            throw ioe;
        }
    }

    /**
     * Read data from the source, filling the array starting at the offset, and reading up to {@code len} bytes.
     *
     * If the end is reached or an {@link IOException} is thrown, the input stream will be closed.
     *
     * @return number of bytes read, or -1 if the end has been reached
     * @throws IOException if there is a problem reading the input stream
     */
    private synchronized int readFromSource(byte[] b, int off, int len) throws IOException {
        try {
            int read = source.read(b, off, len);
            if (read == -1) {
                buffer.closeSource();
            } else {
                buffer.write(b, off, read);
            }
            return read;
        } catch(IOException ioe) {
            buffer.closeSource();
            throw ioe;
        }
    }

    /**
     * Create another input stream reading the same data from the beginning.
     * @return new input stream
     * @throws FileNotFoundException if the file is not found
     */
    public MultiplexerInputStream getInputStream() throws FileNotFoundException {
        return buffer.getInputStream();
    }

    /**
     * Input stream that can also provide the number of bytes read.
     */
    public static abstract class MultiplexerInputStream extends InputStream {
        protected volatile boolean isStreamClosed = false;

        /**
         * Get the number of bytes read so far.
         * @return number of bytes read
         */
        public abstract long getBytesRead();

        @Override
        public synchronized void close() {
            isStreamClosed = true;
        }
    }

    /**
     * Interface for a buffer to store the source data so it can be read again.
     */
    public interface Buffer {
        /**
         * Set the multiplexer. Needs to be called once and only once
         * @param multiplexer multiplexer using the buffer
         */
        void setMultiplexer(BufferedInputStreamMultiplexer multiplexer);

        /**
         * Write data into the buffer.
         * @param b array containing the data
         * @param off start offset
         * @param len number of bytes
         * @throws IOException if there is a problem writing into the buffer
         */
        void write(byte[] b, int off, int len) throws IOException;

        /**
         * Write a single byte into the buffer.
         * @param b byte to write
         * @throws IOException if there is a problem writing into the buffer
         */
        void write(int b) throws IOException;

        /**
         * Indicate that the source input stream is closed.
         */
        void closeSource();

        /**
         * Get a new input stream reading data from this buffer.
         * @return new input stream
         * @throws FileNotFoundException if the file is not found
         */
        MultiplexerInputStream getInputStream() throws FileNotFoundException;

        /**
         * Return true if the source input stream is closed.
         * @return true if the source input stream is closed.
         */
        boolean isSourceClosed();

        /**
         * Return the number of bytes written into the buffer.
         * @return number of bytes written
         */
        long getBytesWritten();
    }

    /**
     * Abstract buffer, implementing shared functionality.
     */
    protected static abstract class AbstractBuffer implements Buffer {
        protected volatile BufferedInputStreamMultiplexer multiplexer;

        @Getter
        protected volatile long bytesWritten = 0;

        @Getter
        protected volatile boolean isSourceClosed = false;

        @Override
        public void setMultiplexer(BufferedInputStreamMultiplexer multiplexer) {
            if (this.multiplexer != null) {
                throw new IllegalArgumentException("multiplexer can only be set once");
            }
            this.multiplexer = multiplexer;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public synchronized void closeSource() {
            isSourceClosed = true;
        }
    }

    /**
     * Memory-backed buffer.
     */
    public static class MemoryBuffer extends AbstractBuffer {
        private byte[] buf;

        /**
         * Create a memory-backed buffer with default initial size (8 kiB).
         */
        public MemoryBuffer() {
            this(8 * 1024);
        }

        /**
         * Create a memory-backed buffer with the provided initial size.
         * @param initialSizeInBytes initial size in bytes
         */
        public MemoryBuffer(int initialSizeInBytes) {
            this.buf = new byte[initialSizeInBytes];
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) {
            while (len > 0) {
                long remainingInBuffer = buf.length - bytesWritten;
                if (remainingInBuffer == 0) {
                    byte[] newBuffer = new byte[buf.length * 2];
                    if (newBuffer.length <= buf.length) {
                        throw new IllegalStateException("Buffer size overflow");
                    }
                    System.arraycopy(buf, 0, newBuffer, 0, buf.length);
                    buf = newBuffer;
                    remainingInBuffer = buf.length - bytesWritten;;
                }
                long bytesToCopy = Math.min(len, remainingInBuffer);
                System.arraycopy(b, off, buf, (int) bytesWritten, (int) bytesToCopy);
                bytesWritten += bytesToCopy;
                off += bytesToCopy;
                len -= bytesToCopy;
            }
        }

        @Override
        public MultiplexerInputStream getInputStream() {
            return new MemoryBufferInputStream();
        }

        protected class MemoryBufferInputStream extends MultiplexerInputStream implements Closeable, AutoCloseable {
            @Getter
            private long bytesRead = 0;

            @Override
            public int read(byte[] b) throws IOException {
                return read(b, 0, b.length);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                synchronized (MemoryBuffer.this) {
                    if (isStreamClosed) {
                        return -1;
                    }

                    int bytesReturned = 0;
                    long bytesAvailable = Math.min(len, bytesWritten - bytesRead);
                    if (bytesAvailable > 0) {
                        // more has been read already
                        System.arraycopy(buf, (int) bytesRead, b, off, (int) bytesAvailable);
                        off += bytesAvailable;
                        len -= bytesAvailable;
                        bytesRead += bytesAvailable;
                        bytesReturned += bytesAvailable;
                    }

                    while(len > 0) {
                        if (isSourceClosed) {
                            break;
                        }
                        int nextByte = read();
                        if (nextByte == -1) {
                            break;
                        }
                        b[off++] = (byte) nextByte;
                        len--;
                        bytesReturned++;
                    }

                    if (bytesReturned == 0 && isSourceClosed) {
                        return -1;
                    }

                    return bytesReturned;
                }
            }

            @Override
            public int read() throws IOException {
                synchronized (MemoryBuffer.this) {
                    if (isStreamClosed) {
                        return -1;
                    }

                    if (bytesRead < bytesWritten) {
                        // more has been read already
                        return buf[(int)bytesRead++] & 0xff;
                    }
                    if (isSourceClosed) {
                        // we're at the end
                        return -1;
                    }
                    // we can read more
                    int read = multiplexer.readFromSource();
                    if (read == -1) {
                        return -1;
                    }
                    ++bytesRead;
                    return read & 0xff;
                }
            }
        }
    }

    /**
     * File-backed buffer.
     */
    public static class FileBuffer extends AbstractBuffer {
        @Getter
        private final RandomAccessFile bufferFile;
        private final FileChannel channel;

        /**
         * Creates a file-backed buffer, storing the data in the provided file.
         * @param bufferFile file used to store the data
         * @throws FileNotFoundException if the file is not found
         */
        public FileBuffer(File bufferFile) throws FileNotFoundException {
            this.bufferFile = new RandomAccessFile(bufferFile, "rw");
            this.channel = this.bufferFile.getChannel();
        }

        @Override
        public synchronized void write(byte[] b, int off, int len) throws IOException {
            try {
                ByteBuffer bb = ByteBuffer.wrap(b, off, len);
                // writing is the only operation that changes the channel's current position
                channel.write(bb);
                bytesWritten += len;
                LOG.trace("Wrote {} more bytes, {} bytes written in total", len, bytesWritten);
            } catch(IOException ioe) {
                closeSource();
                throw ioe;
            }
        }

        @Override
        public MultiplexerInputStream getInputStream() {
            return new FileBufferInputStream();
        }

        protected class FileBufferInputStream extends MultiplexerInputStream implements Closeable, AutoCloseable {
            @Getter
            private long bytesRead = 0;

            @Override
            public int read(byte[] b) throws IOException {
                return read(b, 0, b.length);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                synchronized (FileBuffer.this) {
                    if (isStreamClosed) {
                        return -1;
                    }
                    int bytesReturned = 0;
                    int bytesAvailable = (int) Math.min(Integer.MAX_VALUE, Math.min(len, bytesWritten - bytesRead));
                    if (bytesAvailable > 0) {
                        // more has been read already
                        ByteBuffer bb = ByteBuffer.wrap(b, off, bytesAvailable);
                        // this reads starting at bytesRead, and does not modify the channel's current position
                        int actuallyTransferred = channel.read(bb, bytesRead);
                        assert actuallyTransferred == bytesAvailable;
                        off += bytesAvailable;
                        len -= bytesAvailable;
                        bytesRead += bytesAvailable;
                        bytesReturned += bytesAvailable;
                    }

                    while(len > 0) {
                        if (isSourceClosed) {
                            break;
                        }
                        int bytesReadFromSource = multiplexer.readFromSource(b, off, len);
                        if (bytesReadFromSource == -1) {
                            break;
                        }
                        len -= bytesReadFromSource;
                        off += bytesReadFromSource;
                        bytesRead += bytesReadFromSource;
                        bytesReturned += bytesReadFromSource;
                    }

                    if (bytesReturned == 0 && isSourceClosed) {
                        return -1;
                    }

                    LOG.trace("FileBufferInputStream {} read {} bytes, {} bytes read in total",
                              System.identityHashCode(this), bytesReturned, bytesRead);

                    return bytesReturned;
                }
            }

            @Override
            public int read() throws IOException {
                synchronized (FileBuffer.this) {
                    if (isStreamClosed) {
                        return -1;
                    }
                    if (bytesRead < bytesWritten) {
                        // more has been read already
                        ByteBuffer bb = ByteBuffer.allocate(1);
                        // this reads starting at bytesRead, and does not modify the channel's current position
                        int read = channel.read(bb, bytesRead);
                        if (read == -1) {
                            throw new IllegalStateException("There should have been more bytes available, but read() returned -1");
                        }
                        ++bytesRead;
                        bb.position(0);

                        LOG.trace("FileBufferInputStream {} read 1 byte, {} bytes read in total",
                                  System.identityHashCode(this), bytesRead);

                        return bb.get();
                    }
                    if (isSourceClosed) {
                        // we're at the end
                        return -1;
                    }
                    // we can read more
                    int read = multiplexer.readFromSource();
                    if (read != -1) {
                        ++bytesRead;
                    }

                    LOG.trace("FileBufferInputStream {} read 1 byte, {} bytes read in total",
                              System.identityHashCode(this), bytesRead);

                    return read;
                }
            }
        }
    }
}
