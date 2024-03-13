package com.oracle.bmc.hdfs.store;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.oracle.bmc.hdfs.BmcProperties;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BmcParallelReadAheadFSInputStreamTest {

    @Mock
    private ObjectStorage objectStorage;
    @Mock
    private FileStatus status;
    @Mock private BmcPropertyAccessor mockPropAccessor;
    @Mock private BmcPropertyAccessor.Accessor<Integer> mockIntegerAccessor;
    private FileSystem.Statistics statistics;

    @Before
    public void setup() {
        when(mockIntegerAccessor.get(eq(BmcProperties.NUM_READ_AHEAD_THREADS))).thenReturn(10);
        when(mockPropAccessor.asInteger()).thenReturn(mockIntegerAccessor);
        statistics = new FileSystem.Statistics("oci");
    }

    @Test
    public void testReadSingleByte() throws IOException {
        int blockSize = 10;
        int blockCount = 5;
        int fileSize = blockSize * blockCount;
        byte[] contents = new byte[fileSize];
        Random random = new Random();

        // Fill the byte array with random values
        random.nextBytes(contents);

        // Generate unique indices to place bytes with values from 0x80 to 0xFF
        // We want to make sure that the stream read does not return negative values except the real EOF
        Set<Integer> uniqueIndices = new HashSet<>();
        while (uniqueIndices.size() < fileSize / 2) {
            int index = random.nextInt(fileSize / 2);
            uniqueIndices.add(index);
        }
        // Assign random values from 0x80 to 0xFF to the chosen indices
        for (int index : uniqueIndices) {
            contents[index] = (byte) (random.nextInt(128) + 128);
        }

        BmcParallelReadAheadFSInputStream inputStream = createInputStream(fileSize, contents, blockSize, blockCount);
        for (int i = 0; i < fileSize; i++) {
            int val = inputStream.read();
            Assert.assertEquals(Byte.toUnsignedInt(contents[i]), val);
        }
        Assert.assertEquals(-1, inputStream.read());
    }

    @Test
    public void testReadSingleBlockFile() throws IOException {
        int blockSize = 10;
        int blockCount = 1;
        int fileSize = blockSize * blockCount;

        byte[] contents = new byte[fileSize];
        new Random().nextBytes(contents);

        BmcParallelReadAheadFSInputStream inputStream = createInputStream(fileSize, contents, blockSize, blockCount);

        byte[] readContent = new byte[fileSize];
        int bytesRead = inputStream.read(readContent);

        Assert.assertEquals(fileSize, bytesRead);
        Assert.assertArrayEquals(contents, readContent);
        Assert.assertEquals(-1, inputStream.read());
    }

    @Test
    public void testReadMultiBlockFile() throws IOException {
        int blockSize = 10;
        int blockCount = 5;
        int fileSize = blockSize * blockCount;

        byte[] contents = new byte[fileSize];
        new Random().nextBytes(contents);

        BmcParallelReadAheadFSInputStream inputStream = createInputStream(fileSize, contents, blockSize, blockCount);

        byte[] readContent = new byte[fileSize];
        int bytesRead = 0;
        int totalBytesRead = 0;
        while (totalBytesRead < fileSize && (bytesRead = inputStream.read(readContent, totalBytesRead,
                fileSize - totalBytesRead)) != -1) {
            totalBytesRead += bytesRead;
        }

        Assert.assertEquals(fileSize, totalBytesRead);
        Assert.assertArrayEquals(contents, readContent);
        Assert.assertEquals(-1, inputStream.read());
    }

    @Test
    public void testReadFileFromOffset() throws IOException {
        int blockSize = 10;
        int blockCount = 5;
        int fileSize = blockSize * blockCount;
        byte[] contents = new byte[fileSize];
        new Random().nextBytes(contents);

        BmcParallelReadAheadFSInputStream inputStream = createInputStream(fileSize, contents, blockSize, blockCount);

        // Set the offset
        int offset = 15;
        inputStream.seek(offset);

        byte[] readContent = new byte[fileSize - offset];
        int bytesRead = 0;
        int totalBytesRead = 0;
        while (totalBytesRead < fileSize - offset && (bytesRead = inputStream.read(readContent, totalBytesRead,
                fileSize - offset - totalBytesRead)) != -1) {
            totalBytesRead += bytesRead;
        }

        Assert.assertEquals(fileSize - offset, totalBytesRead);
        Assert.assertArrayEquals(Arrays.copyOfRange(contents, offset, fileSize), readContent);
        Assert.assertEquals(-1, inputStream.read());
    }

    @Test
    public void testSeek() throws IOException {
        int blockSize = 10;
        int blockCount = 10;
        int fileSize = blockSize * blockCount;
        byte[] contents = new byte[fileSize];
        new Random().nextBytes(contents);

        BmcParallelReadAheadFSInputStream inputStream = createInputStream(fileSize, contents, blockSize, blockCount);

        // Seek to different offsets and verify the position
        inputStream.seek(5);
        Assert.assertEquals(5, inputStream.getPos());

        inputStream.seek(20);
        Assert.assertEquals(20, inputStream.getPos());

        inputStream.seek(30);
        Assert.assertEquals(30, inputStream.getPos());
    }

    @Test
    public void testReadFully() throws IOException {
        int blockSize = 10;
        int blockCount = 3;
        int fileSize = blockSize * blockCount;

        byte[] contents = new byte[fileSize];
        new Random().nextBytes(contents);

        BmcParallelReadAheadFSInputStream inputStream = createInputStream(fileSize, contents, blockSize, blockCount);

        // Read some bytes from the input stream to change the current position
        byte[] initialReadContent = new byte[blockSize];
        int bytesRead = inputStream.read(initialReadContent, 0, blockSize);
        Assert.assertEquals(blockSize, bytesRead);

        // Use readFully to read the remaining contents
        byte[] readContent = new byte[fileSize - blockSize];
        inputStream.readFully(blockSize, readContent, 0, fileSize - blockSize);

        // Check that readFully read the correct content
        byte[] expectedReadContent = Arrays.copyOfRange(contents, blockSize, fileSize);
        Assert.assertArrayEquals(expectedReadContent, readContent);

        // Check that the current position in the input stream didn't change
        byte[] currentPositionContent = new byte[blockSize];
        bytesRead = inputStream.read(currentPositionContent, 0, blockSize);
        Assert.assertEquals(blockSize, bytesRead);
        byte[] expectedCurrentPositionContent = Arrays.copyOfRange(contents, blockSize, 2 * blockSize);
        Assert.assertArrayEquals(expectedCurrentPositionContent, currentPositionContent);
    }

    @Test
    public void testReadFileWithRandomOffsetsAndLengths() throws IOException {
        int fileSize = 100;
        byte[] contents = new byte[fileSize];
        new Random().nextBytes(contents);
        int ociReadAheadBlockSize = 64 * 1024 * 1024; // 64 MB
        int ociReadAheadBlockCount = 4;

        BmcParallelReadAheadFSInputStream inputStream = createInputStream(fileSize, contents, ociReadAheadBlockSize,
                ociReadAheadBlockCount);

        List<Pair<Integer, Integer>> offsetsAndLengths = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            int offset = random.nextInt(fileSize);
            int length = random.nextInt(fileSize - offset) + 1;
            offsetsAndLengths.add(Pair.of(offset, length));
        }

        // Read the file for each offset and length pair
        for (Pair<Integer, Integer> offsetAndLength : offsetsAndLengths) {
            int offset = offsetAndLength.getLeft();
            int length = offsetAndLength.getRight();
            byte[] readContent = new byte[length];
            inputStream.seek(offset);

            int bytesRead = 0;
            int totalBytesRead = 0;
            while (totalBytesRead < length && (bytesRead = inputStream.read(readContent, totalBytesRead, length - totalBytesRead)) != -1) {
                totalBytesRead += bytesRead;
            }

            Assert.assertEquals(length, totalBytesRead);
            byte[] expectedContent = Arrays.copyOfRange(contents, offset, offset + length);
            Assert.assertArrayEquals(expectedContent, readContent);
        }
    }

    @Test
    public void testReadAtPosition() throws IOException {
        int blockSize = 10;
        int blockCount = 10;
        int fileSize = blockSize * blockCount;

        byte[] contents = new byte[fileSize];
        new Random().nextBytes(contents);

        BmcParallelReadAheadFSInputStream inputStream = createInputStream(fileSize, contents, blockSize, blockCount);

        // Test reading at different positions
        int[][] positionsAndLengths = {
                {5, 1},    // Seek ahead but still within the same block
                {10, 5},   // Seek ahead larger than block size but less than blockSize * blockCount
                {35, 5},   // Seek forwards to position 35
                {20, 5},   // Seek backwards
                {75, 5}    // Seek ahead larger than blockSize * blockCount
        };

        for (int[] positionAndLength : positionsAndLengths) {
            int position = positionAndLength[0];
            int lengthToRead = positionAndLength[1];

            byte[] buffer = new byte[lengthToRead];
            int bytesRead = inputStream.readAtPosition(position, buffer, 0, lengthToRead);

            Assert.assertEquals(lengthToRead, bytesRead);
            byte[] expectedContent = Arrays.copyOfRange(contents, position, position + lengthToRead);
            Assert.assertArrayEquals(expectedContent, buffer);
        }
    }

    @Test
    public void testReadAtPositionDoesNotChangeCurrentPosition() throws IOException {
        int blockSize = 10;
        int blockCount = 5;
        int fileSize = blockSize * blockCount;

        byte[] contents = new byte[fileSize];
        new Random().nextBytes(contents);

        BmcParallelReadAheadFSInputStream inputStream = createInputStream(fileSize, contents, blockSize, blockCount);

        // Set the current position
        int currentPosition = 10;
        inputStream.seek(currentPosition);

        // Test reading at different positions
        int[] positions = { 5, 20, 30 };
        int lengthToRead = 5;

        for (int position : positions) {
            byte[] buffer = new byte[lengthToRead];
            int bytesRead = inputStream.readAtPosition(position, buffer, 0, lengthToRead);

            // Verify that the current position remains unchanged
            Assert.assertEquals(currentPosition, inputStream.getPos());
        }
    }

    private ExecutorService createExecutorService() {
        final Integer numThreadsForReadaheadOperations =
                mockPropAccessor.asInteger().get(BmcProperties.NUM_READ_AHEAD_THREADS);
        final ExecutorService executorService;
        if (numThreadsForReadaheadOperations == null
                || numThreadsForReadaheadOperations <= 1) {
            executorService =
                    Executors.newFixedThreadPool(
                            1,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("bmcs-hdfs-readahead-%d")
                                    .build());
        } else {
            executorService =
                    Executors.newFixedThreadPool(
                            numThreadsForReadaheadOperations,
                            new ThreadFactoryBuilder()
                                    .setDaemon(true)
                                    .setNameFormat("bmcs-hdfs-readahead-%d")
                                    .build());
        }
        return executorService;
    }

    private BmcParallelReadAheadFSInputStream createInputStream(int fileSize, byte[] contents, int blockSize, int blockCount) {
        when(status.getLen()).thenReturn((long) fileSize);

        when(objectStorage.getObject(any(GetObjectRequest.class)))
                .thenAnswer(invocation -> {
                    GetObjectRequest request = (GetObjectRequest) invocation.getArguments()[0];
                    int startByte = request.getRange().getStartByte().intValue();
                    int endByte = request.getRange().getEndByte().intValue();
                    ByteArrayInputStream inputStream = createInputStream(contents, startByte, endByte);
                    return GetObjectResponse.builder().inputStream(inputStream).build();
                });

        Supplier<GetObjectRequest.Builder> requestBuilder = () ->
                GetObjectRequest.builder().objectName("testObject");

        return new BmcParallelReadAheadFSInputStream(
                objectStorage, status, requestBuilder, statistics, createExecutorService(), blockSize, blockCount);
    }

    private ByteArrayInputStream createInputStream(byte[] contents, int startByte, int endByte) {
        return new ByteArrayInputStream(contents, startByte, endByte - startByte + 1);
    }
}