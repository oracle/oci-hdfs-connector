/**
 * Copyright (c) 2016, 2025, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.contract;

import com.oracle.bmc.hdfs.IntegrationTestCategory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Category({IntegrationTestCategory.class})
public class TestBmcContractBatchDelete extends TestBmcFileSystemContract {

    public static Path INITIAL_WORKING_DIRECTORY;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract.BatchDelete contract = new BmcContract.BatchDelete(configuration);
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
        fs.setWorkingDirectory(INITIAL_WORKING_DIRECTORY);
    }

    @Override
    protected int getGlobalTimeout() {
        return (int) TimeUnit.SECONDS.toMillis(3600);
    }

    @Test
    public void testBatchDeleteSingleFile() throws IOException {
        Path testDir = path("testBatchDeleteSingle");
        Path file = new Path(testDir, "single-file.txt");
        ContractTestUtils.createFile(fs, file, true, "test data".getBytes());

        Assert.assertTrue("Test directory should exist", fs.exists(testDir));
        Assert.assertTrue("File should exist", fs.exists(file));
        Assert.assertEquals("Should have 1 file", 1, fs.listStatus(testDir).length);

        boolean deleted = fs.delete(testDir, true);

        Assert.assertTrue("Delete should return true", deleted);
        Assert.assertFalse("File should not exist after delete", fs.exists(file));
        Assert.assertFalse("Directory should not exist after delete", fs.exists(testDir));
    }

    @Test
    public void testBatchDeleteSmallSet() throws IOException {
        Path testDir = path("testBatchDeleteSmall");

        for (int i = 0; i < 10; i++) {
            Path file = new Path(testDir, "file" + i + ".txt");
            ContractTestUtils.createFile(fs, file, true, "test data".getBytes());
        }

        Assert.assertTrue("Test directory should exist", fs.exists(testDir));
        Assert.assertEquals("Should have 10 files", 10, fs.listStatus(testDir).length);

        boolean deleted = fs.delete(testDir, true);

        Assert.assertTrue("Delete should return true", deleted);
        Assert.assertFalse("Directory should not exist after delete", fs.exists(testDir));
    }

    @Test
    public void testBatchDeleteLargeSet() throws IOException {
        Path testDir = path("testBatchDeleteLarge");
        int fileCount = 100;

        for (int i = 0; i < fileCount; i++) {
            Path file = new Path(testDir, "file" + i + ".txt");
            ContractTestUtils.createFile(fs, file, true, "test data".getBytes());
        }

        Assert.assertTrue("Test directory should exist", fs.exists(testDir));
        Assert.assertEquals("Should have " + fileCount + " files", fileCount, fs.listStatus(testDir).length);

        boolean deleted = fs.delete(testDir, true);

        Assert.assertTrue("Delete should return true", deleted);
        Assert.assertFalse("Directory should not exist after delete", fs.exists(testDir));
    }

    @Test
    public void testBatchDeleteNestedDirectories() throws IOException {
        Path baseDir = path("testBatchDeleteNested");
        Path level1 = new Path(baseDir, "level1");
        Path level2 = new Path(level1, "level2");
        Path file = new Path(level2, "file.txt");
        ContractTestUtils.createFile(fs, file, true, "test data".getBytes());

        Path siblingDir = path("testBatchDeleteNestedSibling");
        Path siblingFile = new Path(siblingDir, "sibling-file.txt");
        ContractTestUtils.createFile(fs, siblingFile, true, "sibling data".getBytes());

        Assert.assertTrue("Base directory should exist", fs.exists(baseDir));
        Assert.assertTrue("Level 1 directory should exist", fs.exists(level1));
        Assert.assertTrue("Level 2 directory should exist", fs.exists(level2));
        Assert.assertTrue("File should exist", fs.exists(file));
        Assert.assertTrue("Sibling directory should exist", fs.exists(siblingDir));
        Assert.assertTrue("Sibling file should exist", fs.exists(siblingFile));

        boolean deleted = fs.delete(baseDir, true);

        Assert.assertTrue("Delete should return true", deleted);
        Assert.assertFalse("Base directory should not exist", fs.exists(baseDir));
        Assert.assertFalse("Level 1 directory should not exist", fs.exists(level1));
        Assert.assertFalse("Level 2 directory should not exist", fs.exists(level2));
        Assert.assertFalse("File should not exist", fs.exists(file));

        Assert.assertTrue("Sibling directory should still exist", fs.exists(siblingDir));
        Assert.assertTrue("Sibling file should still exist", fs.exists(siblingFile));

        fs.delete(siblingDir, true);
    }

    @Test
    public void testBatchDeleteDirectoriesOnly() throws IOException {
        Path baseDir = path("testBatchDeleteDirsOnly");
        Path level1 = new Path(baseDir, "level1");
        Path level2 = new Path(level1, "level2");
        Path level3 = new Path(level2, "level3");

        fs.mkdirs(level3);
        fs.mkdirs(new Path(baseDir, "sibling1"));
        fs.mkdirs(new Path(baseDir, "sibling2"));

        Path siblingOutside = path("testBatchDeleteDirsOnlySibling");
        fs.mkdirs(new Path(siblingOutside, "subfolder"));
        ContractTestUtils.createFile(fs, new Path(siblingOutside, "sibling-file.txt"),
                true, "test data".getBytes());

        Assert.assertTrue("Base directory should exist", fs.exists(baseDir));
        Assert.assertTrue("Level 1 should exist", fs.exists(level1));
        Assert.assertTrue("Level 2 should exist", fs.exists(level2));
        Assert.assertTrue("Level 3 should exist", fs.exists(level3));
        Assert.assertTrue("Sibling 1 should exist", fs.exists(new Path(baseDir, "sibling1")));
        Assert.assertTrue("Sibling 2 should exist", fs.exists(new Path(baseDir, "sibling2")));
        Assert.assertTrue("Sibling outside should exist", fs.exists(siblingOutside));

        boolean deleted = fs.delete(baseDir, true);

        Assert.assertTrue("Delete should return true", deleted);
        Assert.assertFalse("Base directory should not exist", fs.exists(baseDir));
        Assert.assertFalse("Level 1 should not exist", fs.exists(level1));
        Assert.assertFalse("Level 2 should not exist", fs.exists(level2));
        Assert.assertFalse("Level 3 should not exist", fs.exists(level3));

        Assert.assertTrue("Sibling outside baseDir should still exist", fs.exists(siblingOutside));
        Assert.assertTrue("Sibling file should still exist",
                fs.exists(new Path(siblingOutside, "sibling-file.txt")));
        Assert.assertTrue("Sibling subfolder should still exist",
                fs.exists(new Path(siblingOutside, "subfolder")));

        fs.delete(siblingOutside, true);
    }

    @Test
    public void testBatchDeleteEmptyDirectory() throws IOException {
        Path emptyDir = path("testBatchDeleteEmpty");

        fs.mkdirs(emptyDir);
        Assert.assertTrue("Empty directory should exist", fs.exists(emptyDir));

        boolean deleted = fs.delete(emptyDir, true);

        Assert.assertTrue("Delete should return true", deleted);
        Assert.assertFalse("Empty directory should not exist", fs.exists(emptyDir));
    }

    @Test
    public void testConcurrentBatchDeleteDifferentFiles() throws Exception {
        Path testDir = path("testConcurrentDifferent");
        int numThreads = 10;
        int filesPerThread = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        List<Exception> exceptions = new ArrayList<>();

        for (int t = 0; t < numThreads; t++) {
            Path threadDir = new Path(testDir, "thread" + t);
            for (int f = 0; f < filesPerThread; f++) {
                Path file = new Path(threadDir, "file" + f + ".txt");
                ContractTestUtils.createFile(fs, file, true, "test data".getBytes());
            }
        }

        Assert.assertEquals("Should have " + numThreads + " subdirectories",
                numThreads, fs.listStatus(testDir).length);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();

                    Path threadDir = new Path(testDir, "thread" + threadId);
                    boolean deleted = fs.delete(threadDir, true);

                    if (deleted) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        startLatch.countDown();

        completionLatch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        Assert.assertTrue("No exceptions should occur", exceptions.isEmpty());
        Assert.assertEquals("All delete operations should succeed", numThreads, successCount.get());

        for (int t = 0; t < numThreads; t++) {
            Path threadDir = new Path(testDir, "thread" + t);
            Assert.assertFalse("Thread " + t + " directory should be deleted", fs.exists(threadDir));
        }

        Assert.assertTrue("Parent testDir should still exist after deleting all children",
                fs.exists(testDir));

        Path newFile = new Path(testDir, "new-file-after-delete.txt");
        ContractTestUtils.createFile(fs, newFile, true, "test data".getBytes());
        Assert.assertTrue("Should be able to create files in parent after child deletion",
                fs.exists(newFile));

        fs.delete(testDir, true);
    }

    @Test
    public void testConcurrentBatchDeleteSameFiles() throws Exception {
        Path testDir = path("testConcurrentSame");
        int numThreads = 10;
        int numFiles = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completionLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        List<Exception> exceptions = new ArrayList<>();

        for (int f = 0; f < numFiles; f++) {
            Path file = new Path(testDir, "file" + f + ".txt");
            ContractTestUtils.createFile(fs, file, true, "test data".getBytes());
        }

        Assert.assertEquals("Should have " + numFiles + " files",
                numFiles, fs.listStatus(testDir).length);

        for (int t = 0; t < numThreads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    boolean deleted = fs.delete(testDir, true);
                    if (deleted) {
                        successCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    synchronized (exceptions) {
                        exceptions.add(e);
                    }
                } finally {
                    completionLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        completionLatch.await(60, TimeUnit.SECONDS);
        executor.shutdown();

        Assert.assertTrue("At least one delete operation should succeed", successCount.get() >= 1);
        Assert.assertFalse("Test directory should be deleted", fs.exists(testDir));
    }

    @Test
    public void testBatchDeleteIsolation() throws IOException {
        Path testRoot = path("testBatchDeleteIsolation");
        Path child1 = new Path(testRoot, "child1");
        Path child2 = new Path(testRoot, "child2");
        Path child3 = new Path(testRoot, "child3");

        ContractTestUtils.createFile(fs, new Path(child1, "file1.txt"), true, "data1".getBytes());
        ContractTestUtils.createFile(fs, new Path(child1, "file2.txt"), true, "data2".getBytes());
        ContractTestUtils.createFile(fs, new Path(child2, "file3.txt"), true, "data3".getBytes());
        ContractTestUtils.createFile(fs, new Path(child2, "file4.txt"), true, "data4".getBytes());
        ContractTestUtils.createFile(fs, new Path(child3, "file5.txt"), true, "data5".getBytes());

        Assert.assertTrue("Test root should exist", fs.exists(testRoot));
        Assert.assertTrue("Child1 should exist", fs.exists(child1));
        Assert.assertTrue("Child2 should exist", fs.exists(child2));
        Assert.assertTrue("Child3 should exist", fs.exists(child3));
        Assert.assertEquals("Child1 should have 2 files", 2, fs.listStatus(child1).length);
        Assert.assertEquals("Child2 should have 2 files", 2, fs.listStatus(child2).length);
        Assert.assertEquals("Child3 should have 1 file", 1, fs.listStatus(child3).length);

        boolean deleted = fs.delete(child2, true);

        Assert.assertTrue("Delete should return true", deleted);
        Assert.assertFalse("Child2 should be deleted (target)", fs.exists(child2));
        Assert.assertTrue("Test root should still exist (parent preservation)", fs.exists(testRoot));
        Assert.assertTrue("Child1 should still exist (sibling preservation)", fs.exists(child1));
        Assert.assertTrue("Child3 should still exist (sibling preservation)", fs.exists(child3));
        Assert.assertEquals("Child1 should still have 2 files", 2, fs.listStatus(child1).length);
        Assert.assertEquals("Child3 should still have 1 file", 1, fs.listStatus(child3).length);
        Assert.assertTrue("Child1/file1.txt should still exist", fs.exists(new Path(child1, "file1.txt")));
        Assert.assertTrue("Child1/file2.txt should still exist", fs.exists(new Path(child1, "file2.txt")));
        Assert.assertTrue("Child3/file5.txt should still exist", fs.exists(new Path(child3, "file5.txt")));

        fs.delete(testRoot, true);
    }

    @Test
    public void testBatchDeleteDeepNestedIsolation() throws IOException {
        Path root = path("testDeepNested");
        Path level1 = new Path(root, "level1");
        Path level2 = new Path(level1, "level2");
        Path target = new Path(level2, "target");
        Path sibling = new Path(level2, "sibling");
        Path otherBranch = new Path(level1, "otherBranch");

        ContractTestUtils.createFile(fs, new Path(target, "file1.txt"), true, "data1".getBytes());
        ContractTestUtils.createFile(fs, new Path(target, "file2.txt"), true, "data2".getBytes());
        ContractTestUtils.createFile(fs, new Path(sibling, "file3.txt"), true, "data3".getBytes());
        ContractTestUtils.createFile(fs, new Path(otherBranch, "file4.txt"), true, "data4".getBytes());

        Assert.assertTrue("Root should exist", fs.exists(root));
        Assert.assertTrue("Level1 should exist", fs.exists(level1));
        Assert.assertTrue("Level2 should exist", fs.exists(level2));
        Assert.assertTrue("Target should exist", fs.exists(target));
        Assert.assertTrue("Sibling should exist", fs.exists(sibling));
        Assert.assertTrue("OtherBranch should exist", fs.exists(otherBranch));

        boolean deleted = fs.delete(target, true);

        Assert.assertTrue("Delete should return true", deleted);
        Assert.assertFalse("Target should be deleted", fs.exists(target));
        Assert.assertTrue("Root should still exist (ancestor)", fs.exists(root));
        Assert.assertTrue("Level1 should still exist (ancestor)", fs.exists(level1));
        Assert.assertTrue("Level2 should still exist (parent)", fs.exists(level2));
        Assert.assertTrue("Sibling should still exist (same level)", fs.exists(sibling));
        Assert.assertTrue("Sibling/file3.txt should still exist", fs.exists(new Path(sibling, "file3.txt")));
        Assert.assertTrue("OtherBranch should still exist (different branch)", fs.exists(otherBranch));
        Assert.assertTrue("OtherBranch/file4.txt should still exist", fs.exists(new Path(otherBranch, "file4.txt")));

        fs.delete(root, true);
    }
}
