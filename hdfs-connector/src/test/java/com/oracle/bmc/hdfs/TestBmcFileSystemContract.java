/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.oracle.bmc.hdfs.contract.BmcContract;
import com.oracle.bmc.hdfs.store.BmcDataStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({IntegrationTestCategory.class})
public class TestBmcFileSystemContract extends FileSystemContractBaseTest {

    public static Path INITIAL_WORKING_DIRECTORY;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract contract = new BmcContract(configuration);
        contract.init();
        INITIAL_WORKING_DIRECTORY = contract.getTestFileSystem().getWorkingDirectory();
    }

    @Before
    public void setUp() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract contract = new BmcContract(configuration);
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

    @Override
    protected int getGlobalTimeout() {
        return (int) TimeUnit.SECONDS.toMillis(60);
    }

    @Override
    public void testMkdirsWithUmask() throws Exception {
        // not supported
    }

    /*
     * Verify that getting status for a directory path that was not created through the FileSystem works as expected
     * (ie, works without the existence of the placeholder objects).
     */
    public void testGetFileStatusWorksWithExistingObjectsInBucket() throws Exception {
        createExistingFiles();

        FileStatus status = super.fs.getFileStatus(new Path("/existingobjects/fileOnly"));
        assertNotNull(status);
        assertTrue(status.isDirectory());
        assertEquals(0L, status.getLen());

        status = super.fs.getFileStatus(new Path("/existingobjects/fileAndSubdir"));
        assertNotNull(status);
        assertTrue(status.isDirectory());
        assertEquals(0L, status.getLen());

        status = super.fs.getFileStatus(new Path("/existingobjects/subdirOnly"));
        assertNotNull(status);
        assertTrue(status.isDirectory());
        assertEquals(0L, status.getLen());
    }

    /*
     * Verify that listing the status for a directory path that was not created through the FileSystem works as expected
     * (ie, works without the existence of the placeholder objects).
     */
    public void testListFileStatusWorksWithExistingObjectsInBucket() throws Exception {
        createExistingFiles();

        FileStatus[] status = super.fs.listStatus(new Path("/existingobjects/fileOnly"));
        assertNotNull(status);
        assertEquals(1, status.length);
        assertTrue(status[0].isFile());
        assertEquals(new Path("/existingobjects/fileOnly/singleFile.txt"), status[0].getPath());

        status = super.fs.listStatus(new Path("/existingobjects/fileAndSubdir"));
        assertNotNull(status);
        assertEquals(2, status.length);
        assertTrue(status[0].isFile());
        assertEquals(
                new Path("/existingobjects/fileAndSubdir/singleFile.txt"), status[0].getPath());
        assertTrue(status[1].isDirectory());
        assertEquals(new Path("/existingobjects/fileAndSubdir/subdir"), status[1].getPath());

        status = super.fs.listStatus(new Path("/existingobjects/subdirOnly"));
        assertNotNull(status);
        assertEquals(1, status.length);
        assertTrue(status[0].isDirectory());
        assertEquals(new Path("/existingobjects/subdirOnly/subdir"), status[0].getPath());
    }

    /**
     * Verify that listing directories returns the expected values. The connector lists in page sizes of 1000, so we
     * setup a directory of 3001 objects with the following pattern:
     *
     * <pre>
     *   <<<<< page 1
     *   /existingobjects/abc
     *   /existingobjects/def
     *   /existingobjects/list/1
     *   /existingobjects/list/2
     *   /existingobjects/list/...
     *   /existingobjects/list/998
     *   <<<<< page 2
     *   /existingobjects/list/997
     *   /existingobjects/list/1000
     *   /existingobjects/list/nested/1
     *   /existingobjects/list/nested/2
     *   /existingobjects/list/nested/...
     *   /existingobjects/list/nested/500
     *   /existingobjects/test
     *   /existingobjects/zzz/1
     *   /existingobjects/zzz/2
     *   /existingobjects/zzz/...
     *   /existingobjects/zzz/497
     *   <<<<< page 3
     *   /existingobjects/zzz/498
     *   /existingobjects/zzz/499
     *   /existingobjects/zzz/500
     *   /existingobjects/zzzz/1
     *   /existingobjects/zzzz/2
     *   /existingobjects/zzzz/...
     *   /existingobjects/zzzz/497
     *   <<<<< page 4
     *   /existingobjects/zzzzz
     * </pre>
     * <ol>
     * <li>Page 1 should return prefixes [existingobjects/list/'] and objects [abc, def]
     * <li>Page 2 should return prefixes [existingobjects/list/, existingobjects/zzz/] and objects [test]
     * <li>Page 3 should return prefixes [existingobjects/zzz/, existingobjects/zzzz/] and objects []
     * <li>Page 4 should return prefixes [] and objects [zzzzz]
     * </ol>
     */
    public void testListDirectoryWithNextPageTokenOptimization() throws Exception {
        final BmcDataStore dataStore = ((BmcFilesystem) super.fs).getDataStore();
        dataStore.openWriteStream(new Path("/existingobjects/abc"), 1024, null).close();
        dataStore.openWriteStream(new Path("/existingobjects/def"), 1024, null).close();
        dataStore.openWriteStream(new Path("/existingobjects/test"), 1024, null).close();
        dataStore.openWriteStream(new Path("/existingobjects/zzzzz"), 1024, null).close();
        for (int ii = 1; ii <= 1000; ii++) {
            dataStore.openWriteStream(new Path("/existingobjects/list/" + ii), 1024, null).close();
        }
        for (int ii = 1; ii <= 500; ii++) {
            dataStore
                    .openWriteStream(new Path("/existingobjects/list/nested/" + ii), 1024, null)
                    .close();
        }
        for (int ii = 1; ii <= 1000; ii++) {
            dataStore.openWriteStream(new Path("/existingobjects/zzz/" + ii), 1024, null).close();
        }

        for (int ii = 1; ii <= 497; ii++) {
            dataStore.openWriteStream(new Path("/existingobjects/zzzz/" + ii), 1024, null).close();
        }

        FileStatus[] status = super.fs.listStatus(new Path("/existingobjects"));
        assertEquals(7, status.length);
        Set<String> directories = new HashSet<>();
        Set<String> files = new HashSet<>();
        for (FileStatus entry : status) {
            if (entry.isDirectory()) {
                directories.add(entry.getPath().getName());
            } else {
                files.add(entry.getPath().getName());
            }
        }

        Set<String> expectedDirectories = new HashSet<>();
        expectedDirectories.add("list");
        expectedDirectories.add("zzz");
        expectedDirectories.add("zzzz");
        Set<String> expectedFiles = new HashSet<>();
        expectedFiles.add("abc");
        expectedFiles.add("def");
        expectedFiles.add("test");
        expectedFiles.add("zzzzz");

        assertEquals(expectedDirectories, directories);
        assertEquals(expectedFiles, files);
    }

    private void createExistingFiles() throws Exception {
        final BmcDataStore dataStore = ((BmcFilesystem) super.fs).getDataStore();
        dataStore
                .openWriteStream(new Path("/existingobjects/fileOnly/singleFile.txt"), 1024, null)
                .close();
        dataStore
                .openWriteStream(
                        new Path("/existingobjects/fileAndSubdir/singleFile.txt"), 1024, null)
                .close();
        dataStore
                .openWriteStream(
                        new Path("/existingobjects/fileAndSubdir/subdir/singleFile.txt"),
                        1024,
                        null)
                .close();
        dataStore
                .openWriteStream(
                        new Path("/existingobjects/subdirOnly/subdir/singleFile.txt"), 1024, null)
                .close();
    }
}
