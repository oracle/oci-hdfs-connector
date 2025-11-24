/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.contract;

import com.oracle.bmc.hdfs.monitoring.DummyOCIMonitoringPlugin;
import com.oracle.bmc.hdfs.monitoring.OCIMetric;
import com.oracle.bmc.hdfs.monitoring.OCIMetricWithThroughput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestStatsMonitorStreamContract extends TestBmcFileSystemContract {

    public static Path INITIAL_WORKING_DIRECTORY;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract.StatsMonitorStream contract = new BmcContract.StatsMonitorStream(configuration);
        contract.init();
        INITIAL_WORKING_DIRECTORY = contract.getTestFileSystem().getWorkingDirectory();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract.StatsMonitorStream contract = new BmcContract.StatsMonitorStream(configuration);
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

    @Ignore
    @Test
    public void testStatsMonitoringStreams() throws Exception {

        Path homeDirPath = super.fs.getHomeDirectory();
        String homeDir = homeDirPath.toString();

        int[] fileSizesInKB = {100, 1024, 10240};

        for (int i = 0; i < fileSizesInKB.length; i++) {
            byte[] data = getRandomBytes(fileSizesInKB[i]);
            Path testFile = new Path(homeDir + "/testFile");
            uploadFileOneMBFlush(testFile, data);

            byte[] downloadedData = downloadFile(testFile, fileSizesInKB[i] * 1024);
            Assert.assertArrayEquals(data, downloadedData);
            fs.delete(testFile, true);
        }

        DummyOCIMonitoringPlugin pluginInstance = DummyOCIMonitoringPlugin.getInstance();
        List<OCIMetric> metricsList = pluginInstance.getMetrics();

        boolean atleastOneListOp = false;
        boolean atleastOneHeadOp = false;
        boolean atleastOneWriteOp = false;
        boolean atleastOneReadOp = false;
        long totalDataTransfer = (100 + 1024 + 10240) * 1024 * 2;
        long dataTransferFromMetrics = 0l;

        int metricsListSize = metricsList.size();

        for (int i = 0; i < metricsListSize; i++) {
            OCIMetric o  = metricsList.get(i);
            String key = o.getKey();
            if (key.equals("LIST")) {
                atleastOneListOp = true;
            }
            if (key.equals("HEAD")) {
                atleastOneHeadOp = true;
            }
            if (key.equals("WRITE")) {
                atleastOneWriteOp = true;

                OCIMetricWithThroughput ot = (OCIMetricWithThroughput) o;
                dataTransferFromMetrics += ot.getBytesTransferred();
            }
            if (key.equals("READ")) {
                atleastOneReadOp = true;

                OCIMetricWithThroughput ot = (OCIMetricWithThroughput) o;
                dataTransferFromMetrics += ot.getBytesTransferred();
            }

        }

        Assert.assertTrue(atleastOneListOp);
        Assert.assertTrue(atleastOneHeadOp);
        Assert.assertTrue(atleastOneWriteOp);
        Assert.assertTrue(atleastOneReadOp);
        Assert.assertEquals(totalDataTransfer, dataTransferFromMetrics);
    }

    private byte[] downloadFile(Path p, int size) throws IOException {
        byte[] tempBytes = new byte[size];
        int totalBytesDownloaded = 0;

        try (FSDataInputStream fis = fs.open(p)) {
            byte[] tempBuf = new byte[1 * 1024 * 1024];
            int len = 0;
            while ((len = fis.read(tempBuf)) != -1) {
                System.arraycopy(tempBuf, 0, tempBytes, totalBytesDownloaded, len);
                totalBytesDownloaded += len;
            }
        }
        return tempBytes;
    }

    private void uploadFileOneMBFlush(Path p, byte[] data) throws IOException {

        int oneMB = 1 * 1024 * 1024;

        try (FSDataOutputStream fos = fs.create(p)) {

            byte[] buf = new byte[oneMB];
            int lenRead = 0;
            for (int i = 0; i < data.length; i++) {
                fos.write(data[i]);
                if (i > 0 && i % oneMB == 0) {
                    fos.flush();
                }
            }

            fos.flush();
        }
    }

    private byte[] getRandomBytes(int sizeInKB) throws IOException {
        Random rand = new Random();
        byte[] buffer = new byte[sizeInKB * 1024];
        return buffer;
    }
}
