package com.oracle.bmc.hdfs;

import com.oracle.bmc.hdfs.contract.BmcContract;
import com.oracle.bmc.objectstorage.transfer.internal.ChecksumUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestChecksumBmcContractCreate extends TestBmcFileSystemContract {

    @BeforeClass
    public static void setUpStatic() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract.ChecksumWrite contract =
                new BmcContract.ChecksumWrite(configuration);
        contract.init();
        INITIAL_WORKING_DIRECTORY = contract.getTestFileSystem().getWorkingDirectory();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract.InMemory contract = new BmcContract.InMemory(configuration);
        contract.init();
        super.fs = contract.getTestFileSystem();

        // reset the working directory to avoid test-to-test influence
        fs.setWorkingDirectory(INITIAL_WORKING_DIRECTORY);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.fs.delete(new Path("testCreateWithChecksumAndVerifyChecksum"), true);
    }

    @Test
    public void testCreateWithChecksumAndVerifyChecksum() throws Throwable {

        final Path path = this.path("testCreateWithChecksumAndVerifyChecksum");
        final String testData = "123456789";

        try (FSDataOutputStream out = fs.create(path, false, 4096, (short) 1, 1024)) {
            out.writeBytes(testData);
        } catch (Exception e) {
            throw new RuntimeException("Error writing data to file", e);
        }

        try (FSDataInputStream in = fs.open(path)) {
            byte[] buffer = new byte[testData.length()];
            in.readFully(buffer);
            String readData = new String(buffer);
            assertEquals(testData, readData);
        } catch (Exception e) {
            throw new RuntimeException("Error reading data from file", e);
        }

        FileChecksum checksum = fs.getFileChecksum(path);
        assertNotNull(checksum);

        byte[] expectedChecksumBytes =
                Base64.getDecoder().decode(ChecksumUtils.calculateCrc32cChecksum(testData.getBytes(StandardCharsets.UTF_8)));

        assertArrayEquals(expectedChecksumBytes, checksum.getBytes());
        assertEquals("COMPOSITE-CRC32C", checksum.getAlgorithmName());
        byte[] checksumBytes = checksum.getBytes();
        assertNotNull(checksumBytes);
    }
}