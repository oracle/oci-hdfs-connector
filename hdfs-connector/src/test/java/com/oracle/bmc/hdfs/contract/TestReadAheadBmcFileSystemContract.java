/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static org.junit.Assert.assertEquals;

public class TestReadAheadBmcFileSystemContract extends TestBmcFileSystemContract {

    public static Path INITIAL_WORKING_DIRECTORY;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract.ReadAhead contract = new BmcContract.ReadAhead(configuration);
        contract.init();
        INITIAL_WORKING_DIRECTORY = contract.getTestFileSystem().getWorkingDirectory();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract.ReadAhead contract = new BmcContract.ReadAhead(configuration);
        contract.init();
        super.fs = contract.getTestFileSystem();

        // reset the working directory to avoid test-to-test influence
        fs.setWorkingDirectory(INITIAL_WORKING_DIRECTORY);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        super.fs.delete(new Path("/existingobjects"), true);
    }

    @Test
    public void testSimpleReadAhead() throws Exception {

        Path testDirPath = new Path("/test-dir");
        if (fs.exists(testDirPath)) {
            fs.delete(testDirPath, true);
        }

        try {
            Path testFile = new Path("/test-dir/test-file");
            Random rand = new Random();
            byte[] buffer = new byte[10 * 1024];
            rand.nextBytes(buffer);

            Checksum checksum = new CRC32();
            checksum.update(buffer, 0, buffer.length);
            long crc32Checksum = checksum.getValue();

            createAndUploadFile(testFile, buffer);

            // Read the object back using read ahead stream.
            FSDataInputStream fsdi = fs.open(testFile);
            byte[] result = new byte[10 * 1024];
            fsdi.readFully(result, 0, result.length);
            fsdi.close();

            checksum = new CRC32();
            checksum.update(result, 0, result.length);
            long resultCrc32Checksum = checksum.getValue();

            Assert.assertEquals(crc32Checksum, resultCrc32Checksum);

        } finally {
            fs.delete(testDirPath, true);
        }
    }

    /*
      There was a bug in read ahead input stream that byte buffer length being 8, led to code path related to dealing
      with parquet files. The following code tests that for non-parquet files, this bug is fixed.
     */
    @Test
    public void testReadAheadWithEightByteBuffer() throws Exception {
        Path testDirPath = new Path("/test-dir");
        if (fs.exists(testDirPath)) {
            fs.delete(testDirPath, true);
        }

        try {
            Path testFile = new Path("/test-dir/test-file");
            Random rand = new Random();
            byte[] buffer = new byte[8];
            rand.nextBytes(buffer);

            Checksum checksum = new CRC32();
            checksum.update(buffer, 0, buffer.length);
            long crc32Checksum = checksum.getValue();

            createAndUploadFile(testFile, buffer);

            // Read the object back using read ahead stream.
            FSDataInputStream fsdi = fs.open(testFile);

            // Read only 8 bytes at a time.
            byte[] tempResult = new byte[8];
            byte[] result = new byte[8];

            int readLength = -1;
            int i = 0;
            while ((readLength = fsdi.read(tempResult, 0, tempResult.length)) != -1) {
                for (int j = 0; j < readLength; j++) {
                    result[i] = tempResult[j];
                    i++;
                }
            }
            fsdi.close();

            checksum = new CRC32();
            checksum.update(result, 0, result.length);
            long resultCrc32Checksum = checksum.getValue();

            Assert.assertEquals(crc32Checksum, resultCrc32Checksum);

        } finally {
            fs.delete(testDirPath, true);
        }
    }

    private void createAndUploadFile(Path file, byte[] buffer) throws IOException {
        try(FSDataOutputStream fos = fs.create(file)) {
            fos.write(buffer);
            fos.flush();
        }
    }

}
