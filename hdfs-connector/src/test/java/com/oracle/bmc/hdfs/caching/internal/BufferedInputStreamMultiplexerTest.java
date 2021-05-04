package com.oracle.bmc.hdfs.caching.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BufferedInputStreamMultiplexerTest {
    public static final String CONTENT = "Night-time shows us where they are.";
    public static final int TEST_LENGTH = 20 * 1024 * 1024;

    @Test
    public void testMemoryBuffer() throws Exception {
        testBufferHelper(() -> new BufferedInputStreamMultiplexer.MemoryBuffer());
    }

    @Test
    public void testMemoryBufferReadPartial() throws IOException {
        BufferedInputStreamMultiplexer.MemoryBuffer buf = new BufferedInputStreamMultiplexer.MemoryBuffer();

        testBufferReadPartialHelper(buf);
    }

    @Test
    public void testMemoryBufferReadPartialAndClose() throws IOException {
        BufferedInputStreamMultiplexer.MemoryBuffer buf = new BufferedInputStreamMultiplexer.MemoryBuffer();

        testBufferReadPartialAndCloseHelper(buf);
    }

    @Test
    public void testMemoryBufferReadPartialAndClose_Large() throws IOException {
        BufferedInputStreamMultiplexer.Buffer buf = new BufferedInputStreamMultiplexer.MemoryBuffer();

        testBufferReadPartialAndClose_LargeHelper(buf);
    }

    // FileBuffer

    @Test
    public void testFileBuffer() throws Exception {
        testBufferHelper(() -> {
            File bufferFile = Files.createTempFile(this.getClass().getName() + "-", ".tmp").toFile();
            return new BufferedInputStreamMultiplexer.FileBuffer(bufferFile);
        });
    }

    @Test
    public void testFileBufferReadPartial() throws IOException {
        File bufferFile = Files.createTempFile(this.getClass().getName() + "-", ".tmp").toFile();
        BufferedInputStreamMultiplexer.Buffer buf = new BufferedInputStreamMultiplexer.FileBuffer(bufferFile);

        testBufferReadPartialHelper(buf);
    }

    @Test
    public void testFileBufferReadPartialAndClose() throws IOException {
        File bufferFile = Files.createTempFile(this.getClass().getName() + "-", ".tmp").toFile();
        BufferedInputStreamMultiplexer.Buffer buf = new BufferedInputStreamMultiplexer.FileBuffer(bufferFile);

        testBufferReadPartialAndCloseHelper(buf);
    }

    @Test
    public void testFileBufferReadPartialAndClose_Large() throws IOException {
        File bufferFile = Files.createTempFile(this.getClass().getName() + "-", ".tmp").toFile();
        BufferedInputStreamMultiplexer.Buffer buf = new BufferedInputStreamMultiplexer.FileBuffer(bufferFile);

        testBufferReadPartialAndClose_LargeHelper(buf);
    }

    private void testBufferReadPartialAndClose_LargeHelper(BufferedInputStreamMultiplexer.Buffer buf) throws IOException {
        byte[] original = generateTestContent(TEST_LENGTH);
        int[] read = new int[] {0, 0, 0};
        Random r = new Random(12345);
        BufferedInputStreamMultiplexer.MultiplexerInputStream[] isArray = new BufferedInputStreamMultiplexer.MultiplexerInputStream[3];
        ByteArrayOutputStream[] baosArray = new ByteArrayOutputStream[3];

        ByteArrayInputStream source = new ByteArrayInputStream(original);
        BufferedInputStreamMultiplexer multiplexer = new BufferedInputStreamMultiplexer(source, buf);

        isArray[0] = multiplexer.getInputStream();
        baosArray[0] = new ByteArrayOutputStream();
        int readBufLength = 10 * 1024 * 1024;
        byte[] bytes = new byte[readBufLength];
        read[0] += isArray[0].read(bytes);
        baosArray[0].write(bytes, 0, read[0]);
        assertSubArrayEquals(original, baosArray[0]);

        assertEquals(readBufLength, buf.getBytesWritten());
        assertEquals(readBufLength, isArray[0].getBytesRead());

        isArray[1] = multiplexer.getInputStream();
        baosArray[1] = new ByteArrayOutputStream();
        int stream2BytesRead = 5 * 1024 * 1024 + 17;
        read[1] = isArray[1].read(bytes, 0, stream2BytesRead);
        baosArray[1].write(bytes, 0, read[1]);
        assertSubArrayEquals(original, baosArray[1]);

        assertEquals(readBufLength, buf.getBytesWritten());
        assertEquals(stream2BytesRead, isArray[1].getBytesRead());

        isArray[2] = multiplexer.getInputStream();
        baosArray[2] = new ByteArrayOutputStream();
        int stream3BytesRead = 7 * 1024 * 1024 + 31;
        read[2] = isArray[2].read(bytes, 0, stream3BytesRead);
        baosArray[2].write(bytes, 0, read[2]);
        assertSubArrayEquals(original, baosArray[2]);

        assertEquals(readBufLength, buf.getBytesWritten());
        assertEquals(stream3BytesRead, isArray[2].getBytesRead());

        // make a few smaller reads
        for(int i=0; i<1000; ++i) {
            int streamIndex = r.nextInt(3);
            BufferedInputStreamMultiplexer.MultiplexerInputStream is = isArray[streamIndex];

            int bytesToRead = r.nextInt(5000);
            int readHere = is.read(bytes, 0, bytesToRead);
            read[streamIndex] += readHere;

            ByteArrayOutputStream baos = baosArray[streamIndex];
            if (readHere > 0) {
                baos.write(bytes, 0, readHere);
            }
            assertSubArrayEquals(original, baos);
            assertEquals(read[streamIndex], baosArray[streamIndex].size());
        }

        isArray[1].close();

        // make a few more smaller reads
        for(int i=0; i<1000; ++i) {
            int streamIndex = r.nextInt(3);
            BufferedInputStreamMultiplexer.MultiplexerInputStream is = isArray[streamIndex];

            int bytesToRead = r.nextInt(5000);
            int readHere = is.read(bytes, 0, bytesToRead);

            ByteArrayOutputStream baos = baosArray[streamIndex];
            if (readHere > 0) {
                baos.write(bytes, 0, readHere);
                read[streamIndex] += readHere;
            }
            assertSubArrayEquals(original, baos);
            assertEquals(read[streamIndex], baos.size());
        }

        // read the rest
        IOUtils.copy(isArray[0], baosArray[0]);
        assertSubArrayEquals(original, baosArray[0], baosArray[0].size());
        assertEquals(TEST_LENGTH, baosArray[0].size());

        read[1] += IOUtils.copy(isArray[1], baosArray[1]);
        assertSubArrayEquals(original, baosArray[1], baosArray[1].size());
        assertEquals(read[1], baosArray[1].size()); // this one was closed, so it doesn't read everything

        IOUtils.copy(isArray[2], baosArray[2]);
        assertSubArrayEquals(original, baosArray[2], baosArray[2].size());
        assertEquals(TEST_LENGTH, baosArray[2].size());
    }

    @Ignore
    @Test
    public void testBufferReadPartialAndClose_Large_Comparison() throws IOException {
        byte[] original = generateTestContent(TEST_LENGTH);
        int[] read = new int[] {0, 0, 0};
        Random r = new Random(12345);
        InputStream[] isArray = new InputStream[3];
        ByteArrayOutputStream[] baosArray = new ByteArrayOutputStream[3];

        isArray[0] = new ByteArrayInputStream(original);
        baosArray[0] = new ByteArrayOutputStream();
        int readBufLength = 10 * 1024 * 1024;
        byte[] bytes = new byte[readBufLength];
        read[0] += isArray[0].read(bytes);
        baosArray[0].write(bytes, 0, read[0]);
        assertSubArrayEquals(original, baosArray[0]);

        isArray[1] = new ByteArrayInputStream(original);
        baosArray[1] = new ByteArrayOutputStream();
        int stream2BytesRead = 5 * 1024 * 1024 + 17;
        read[1] = isArray[1].read(bytes, 0, stream2BytesRead);
        baosArray[1].write(bytes, 0, read[1]);
        assertSubArrayEquals(original, baosArray[1]);

        isArray[2] = new ByteArrayInputStream(original);
        baosArray[2] = new ByteArrayOutputStream();
        int stream3BytesRead = 7 * 1024 * 1024 + 31;
        read[2] = isArray[2].read(bytes, 0, stream3BytesRead);
        baosArray[2].write(bytes, 0, read[2]);
        assertSubArrayEquals(original, baosArray[2]);

        // make a few smaller reads
        for(int i=0; i<1000; ++i) {
            int streamIndex = r.nextInt(3);
            InputStream is = isArray[streamIndex];

            int bytesToRead = r.nextInt(5000);
            int readHere = is.read(bytes, 0, bytesToRead);
            read[streamIndex] += readHere;

            ByteArrayOutputStream baos = baosArray[streamIndex];
            if (readHere > 0) {
                baos.write(bytes, 0, readHere);
            }
            assertSubArrayEquals(original, baos);
        }

        isArray[1].close(); // turns out, ByteArrayInputStreams ignore close()

        // make a few more smaller reads
        for(int i=0; i<1000; ++i) {
            int streamIndex = r.nextInt(3);
            if (streamIndex == 1) {
                continue; // fake closed stream
            }
            InputStream is = isArray[streamIndex];

            int bytesToRead = r.nextInt(5000);
            int readHere = is.read(bytes, 0, bytesToRead);
            read[streamIndex] += readHere;

            ByteArrayOutputStream baos = baosArray[streamIndex];
            if (readHere > 0) {
                baos.write(bytes, 0, readHere);
            }
            assertSubArrayEquals(original, baos);
        }

        // read the rest
        IOUtils.copy(isArray[0], baosArray[0]);
        assertSubArrayEquals(original, baosArray[0], baosArray[0].size());
        assertEquals(TEST_LENGTH, baosArray[0].size());

        // read[1] += IOUtils.copy(isArray[1], baosArray[1]); // fake closed stream\
        assertSubArrayEquals(original, baosArray[1], baosArray[1].size());
        assertEquals(read[1], baosArray[1].size()); // this one was closed, so it doesn't read everything

        IOUtils.copy(isArray[2], baosArray[2]);
        assertSubArrayEquals(original, baosArray[2], baosArray[2].size());
        assertEquals(TEST_LENGTH, baosArray[2].size());
    }

    // Helpers

    private void testBufferHelper(Callable<BufferedInputStreamMultiplexer.Buffer> bufProvider) throws Exception {
        BufferedInputStreamMultiplexer.Buffer buf = bufProvider.call();
        testBufferHelperSimple(buf);
        buf = bufProvider.call();
        testBufferHelperMix(buf);
    }

    private void testBufferHelperSimple(BufferedInputStreamMultiplexer.Buffer buf) throws IOException {
        ByteArrayInputStream source = new ByteArrayInputStream(CONTENT.getBytes());
        BufferedInputStreamMultiplexer multiplexer = new BufferedInputStreamMultiplexer(source, buf);

        InputStream is1 = multiplexer.getInputStream();
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        IOUtils.copy(is1, baos1);
        assertArrayEquals(CONTENT.getBytes(), baos1.toByteArray());

        InputStream is2 = multiplexer.getInputStream();
        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        IOUtils.copy(is2, baos2);
        assertArrayEquals(CONTENT.getBytes(), baos2.toByteArray());

        InputStream is3 = multiplexer.getInputStream();
        ByteArrayOutputStream baos3 = new ByteArrayOutputStream();
        IOUtils.copy(is3, baos3);
        assertArrayEquals(CONTENT.getBytes(), baos3.toByteArray());
    }

    private void testBufferHelperMix(BufferedInputStreamMultiplexer.Buffer buf) throws IOException {
        ByteArrayInputStream source = new ByteArrayInputStream(CONTENT.getBytes());
        BufferedInputStreamMultiplexer multiplexer = new BufferedInputStreamMultiplexer(source, buf);

        int is1Read = 0;
        int is2Read = 0;
        int is3Read = 0;

        InputStream is1 = multiplexer.getInputStream();
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        is1Read += copy(is1, baos1, 12);
        assertSubArrayEquals(CONTENT.getBytes(), baos1, is1Read);

        InputStream is2 = multiplexer.getInputStream();
        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        is2Read += copy(is2, baos2, 10);
        assertSubArrayEquals(CONTENT.getBytes(), baos2, is2Read);

        InputStream is3 = multiplexer.getInputStream();
        ByteArrayOutputStream baos3 = new ByteArrayOutputStream();
        is3Read += copy(is3, baos3, 14);
        assertSubArrayEquals(CONTENT.getBytes(), baos3, is3Read);

        // 2nd read
        is3Read += copy(is3, baos3, 10);
        assertSubArrayEquals(CONTENT.getBytes(), baos3, is3Read);

        is1Read += copy(is1, baos1, 12);
        assertSubArrayEquals(CONTENT.getBytes(), baos1, is1Read);

        is2Read += copy(is2, baos2, 10);
        assertSubArrayEquals(CONTENT.getBytes(), baos2, is2Read);

        // final read
        IOUtils.copy(is1, baos1);
        assertArrayEquals(CONTENT.getBytes(), baos1.toByteArray());

        IOUtils.copy(is2, baos2);
        assertArrayEquals(CONTENT.getBytes(), baos2.toByteArray());

        IOUtils.copy(is3, baos3);
        assertArrayEquals(CONTENT.getBytes(), baos3.toByteArray());
    }

    private static int copy(InputStream is, OutputStream os, int count) throws IOException {
        byte[] buffer = new byte[1024];
        int toRead = Math.min(buffer.length, count);
        int len = is.read(buffer, 0, toRead);
        int copied = 0;
        while (len != -1 && count > 0) {
            os.write(buffer, 0, len);
            copied += len;
            count -= len;
            toRead = Math.min(buffer.length, count);
            len = is.read(buffer, 0, toRead);
        }
        return copied;

    }

    private void testBufferReadPartialAndCloseHelper(BufferedInputStreamMultiplexer.Buffer buf) throws IOException {
        ByteArrayInputStream source = new ByteArrayInputStream(CONTENT.getBytes());
        BufferedInputStreamMultiplexer multiplexer = new BufferedInputStreamMultiplexer(source, buf);

        InputStream is1 = multiplexer.getInputStream();
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        byte[] b1 = new byte[10];
        int read1 = is1.read(b1);
        baos1.write(b1, 0, read1);
        assertSubArrayEquals(CONTENT.getBytes(), baos1);

        InputStream is2 = multiplexer.getInputStream();
        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        byte[] b2 = new byte[10];
        int read2 = is2.read(b2, 0, 5);
        baos2.write(b2, 0, read2);
        assertSubArrayEquals(CONTENT.getBytes(), baos2);
        is2.close();

        InputStream is3 = multiplexer.getInputStream();
        ByteArrayOutputStream baos3 = new ByteArrayOutputStream();
        byte[] b3 = new byte[10];
        int read3 = is3.read(b3, 0, 7);
        baos3.write(b3, 0, read3);
        assertSubArrayEquals(CONTENT.getBytes(), baos3);

        // read the rest
        IOUtils.copy(is1, baos1);
        assertSubArrayEquals(CONTENT.getBytes(), baos1, CONTENT.getBytes().length); // is2 was closed, is1 reads full

        IOUtils.copy(is2, baos2);
        assertSubArrayEquals(CONTENT.getBytes(), baos2, 5);

        IOUtils.copy(is3, baos3);
        assertSubArrayEquals(CONTENT.getBytes(), baos3, CONTENT.getBytes().length); // is2 was closed, is3 reads full
    }

    private void testBufferReadPartialHelper(BufferedInputStreamMultiplexer.Buffer buf) throws IOException {
        ByteArrayInputStream source = new ByteArrayInputStream(CONTENT.getBytes());
        BufferedInputStreamMultiplexer multiplexer = new BufferedInputStreamMultiplexer(source, buf);

        InputStream is1 = multiplexer.getInputStream();
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        byte[] b1 = new byte[10];
        int read1 = is1.read(b1);
        baos1.write(b1, 0, read1);
        assertSubArrayEquals(CONTENT.getBytes(), baos1);

        InputStream is2 = multiplexer.getInputStream();
        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        byte[] b2 = new byte[10];
        int read2 = is2.read(b2, 0, 5);
        baos2.write(b2, 0, read2);
        assertSubArrayEquals(CONTENT.getBytes(), baos2);

        InputStream is3 = multiplexer.getInputStream();
        ByteArrayOutputStream baos3 = new ByteArrayOutputStream();
        byte[] b3 = new byte[10];
        int read3 = is3.read(b3, 0, 7);
        baos3.write(b3, 0, read3);
        assertSubArrayEquals(CONTENT.getBytes(), baos3);

        // read the rest
        IOUtils.copy(is1, baos1);
        assertArrayEquals(CONTENT.getBytes(), baos1.toByteArray());

        IOUtils.copy(is2, baos2);
        assertArrayEquals(CONTENT.getBytes(), baos2.toByteArray());

        IOUtils.copy(is3, baos3);
        assertArrayEquals(CONTENT.getBytes(), baos3.toByteArray());
    }

    private static void assertSubArrayEquals(byte[] expectedFull, ByteArrayOutputStream baos) {
        assertSubArrayEquals(expectedFull, baos, baos.toByteArray().length);
    }

    private static void assertSubArrayEquals(byte[] expectedFull, ByteArrayOutputStream baos, int length) {
        byte[] actual = baos.toByteArray();
        assertEquals(length, actual.length);
        byte[] expected = new byte[length];
        System.arraycopy(expectedFull, 0, expected, 0, length);
        assertArrayEquals(expected, actual);
    }

    public static byte[] generateTestContent(int length) {
        Random r = new Random();
        byte[] bytes = new byte[length];

        r.nextBytes(bytes);

        return bytes;
    }

}
