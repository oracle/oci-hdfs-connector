/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.oracle.bmc.hdfs.contract.BmcContract;
import com.oracle.bmc.hdfs.store.BmcDataStore;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.FutureDataInputStreamBuilder;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
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
        return (int) TimeUnit.SECONDS.toMillis(1200);
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
                        new Path("/existingobjects/fileAndSubdir/subdir/singleFile.txt"), 1024, null)
                .close();
        dataStore
                .openWriteStream(
                        new Path("/existingobjects/subdirOnly/subdir/singleFile.txt"), 1024, null)
                .close();
    }

    @Test
    public void testSpecialCharsInRenameDirectory() throws Exception {

        Path homeDirPath = super.fs.getHomeDirectory();
        String homeDir = homeDirPath.toString();

        String specialCharsDirPath = homeDirPath + "/specialChars";
        Path specialCharsDir = new Path(specialCharsDirPath);

        if (!super.fs.exists(specialCharsDir)) {
            boolean dirCreated = super.fs.mkdirs(specialCharsDir);
            assertEquals(dirCreated, true);
        }

        try {
            char[] specialChars = {'$', '\\', '^', '.', '*'};
            BmcDataStore dataStore = ((BmcFilesystem) super.fs).getDataStore();

            for (char c : specialChars) {
                Path sourceDir = new Path(specialCharsDirPath + "/source/" + c + "test/");
                Path destDir = new Path(specialCharsDirPath + "/dest/" + c + "test/");
                super.fs.mkdirs(sourceDir);
                dataStore.renameDirectory(sourceDir, destDir);
            }
        } finally {
            super.fs.delete(specialCharsDir, true);
        }
    }

    @Test
    public void testFlatListingOfFiles() throws Exception {
        Path homeDirPath = fs.getHomeDirectory();
        String homeDir = homeDirPath.toString();

        Path testDirPath = new Path(homeDir + "/Dir1Level0");
        if (fs.exists(testDirPath)) {
            fs.delete(testDirPath, true);
        }

        try {
            prepareHierarchy(homeDir);

            Map<String, Integer> fileSizeMap = new HashMap<>();
            fileSizeMap.put("/Dir1Level0/Dir1Level1/Dir1Level2/File1", 1024);
            fileSizeMap.put("/Dir1Level0/Dir1Level1/Dir1Level2/File2", 2048);
            fileSizeMap.put("/Dir1Level0/Dir1Level1/Dir3Level2/File1", 1024);
            fileSizeMap.put("/Dir1Level0/Dir1Level1/File1", 1024);
            fileSizeMap.put("/Dir1Level0/Dir3Level1/Dir1Level2/File1", 1024);
            fileSizeMap.put("/Dir1Level0/Dir3Level1/File1", 1024);
            fileSizeMap.put("/Dir1Level0/Dir4Level1/Dir1Level2/Dir1Level3/File1", 1024);
            fileSizeMap.put("/Dir1Level0/Dir4Level1/Dir1Level2/File1", 1024);

            RemoteIterator<LocatedFileStatus> iter = fs.listFiles(homeDirPath, true);
            int fileCount = 0;
            while (iter.hasNext()) {
                LocatedFileStatus file = iter.next();
                String actualFilePath = file.getPath().toString();
                String path = actualFilePath.substring(homeDir.length(), actualFilePath.length());
                assertNotNull(fileSizeMap.get(path));
                assertEquals(fileSizeMap.get(path).intValue(), file.getLen());
                fileCount++;
            }

            assertEquals(fileCount, fileSizeMap.size());

            iter = fs.listFiles(new Path(homeDir + "/Dir1Level0/Dir1Level1"), true);
            Set<Path> subDirPaths = new HashSet<>();
            while (iter.hasNext()) {
                subDirPaths.add(iter.next().getPath());
            }
            assertTrue(subDirPaths.size() == 4);
            assertTrue(subDirPaths.contains(new Path(homeDir + "/Dir1Level0/Dir1Level1/Dir1Level2/File1")));
            assertTrue(subDirPaths.contains(new Path(homeDir + "/Dir1Level0/Dir1Level1/Dir1Level2/File2")));
            assertTrue(subDirPaths.contains(new Path(homeDir + "/Dir1Level0/Dir1Level1/Dir3Level2/File1")));
            assertTrue(subDirPaths.contains(new Path(homeDir + "/Dir1Level0/Dir1Level1/File1")));

            iter = fs.listFiles(new Path(homeDir + "/Dir1Level0/Dir3Level1"), true);
            subDirPaths.clear();
            while (iter.hasNext()) {
                subDirPaths.add(iter.next().getPath());
            }
            assertTrue(subDirPaths.size() == 2);
            assertTrue(subDirPaths.contains(new Path(homeDir + "/Dir1Level0/Dir3Level1/Dir1Level2/File1")));
            assertTrue(subDirPaths.contains(new Path(homeDir + "/Dir1Level0/Dir3Level1/File1")));

        } finally {
            fs.delete(testDirPath, true);
        }
    }

    private void prepareHierarchy(String homeDir) throws Exception {
        fs.mkdirs(new Path(homeDir + "/Dir1Level0/Dir1Level1/Dir1Level2/Dir1Level3"));
        createAndUploadFile(new Path(homeDir + "/Dir1Level0/Dir1Level1/Dir1Level2/File1"), 1);
        createAndUploadFile(new Path(homeDir + "/Dir1Level0/Dir1Level1/Dir1Level2/File2"), 2);
        fs.mkdirs(new Path(homeDir + "/Dir1Level0/Dir1Level1/Dir2Level2/"));
        fs.mkdirs(new Path(homeDir + "/Dir1Level0/Dir1Level1/Dir3Level2/"));
        createAndUploadFile(new Path(homeDir + "/Dir1Level0/Dir1Level1/Dir3Level2/File1"), 1);
        createAndUploadFile(new Path(homeDir + "/Dir1Level0/Dir1Level1/File1"), 1);
        fs.mkdirs(new Path(homeDir + "/Dir1Level0/Dir2Level1"));

        initNonZeroByteMarkerDirsWithFile(new Path(homeDir + "/Dir1Level0/Dir3Level1/Dir1Level2/File1"));
        initNonZeroByteMarkerDirsWithFile(new Path(homeDir + "/Dir1Level0/Dir3Level1/Dir2Level2/File1"));

        // Delete the "/Dir3Level1/Dir2Level2/File1" to also delete the Dir2Level2 because it was
        // not created with a zero byte marker file.
        fs.delete(new Path(homeDir + "/Dir1Level0/Dir3Level1/Dir2Level2/File1"), false);

        initNonZeroByteMarkerDirsWithFile(new Path(homeDir + "/Dir1Level0/Dir3Level1/File1"));

        initNonZeroByteMarkerDirsWithFile(new Path(homeDir + "/Dir1Level0/Dir4Level1/Dir1Level2/File1"));
        fs.mkdirs(new Path(homeDir + "/Dir1Level0/Dir4Level1/Dir1Level2/Dir1Level3"));
        createAndUploadFile(new Path(homeDir + "/Dir1Level0/Dir4Level1/Dir1Level2/Dir1Level3/File1"), 1);
    }

    private void initNonZeroByteMarkerDirsWithFile(Path file) throws IOException {
        Random rand = new Random();
        BmcDataStore dataStore = ((BmcFilesystem) fs).getDataStore();
        try (OutputStream os = dataStore.openWriteStream(file, 1024, null)) {
            byte[] oneKB = new byte[1024];
            rand.nextBytes(oneKB);
            os.write(oneKB);
            os.flush();
        }
    }

    private void createAndUploadFile(Path file, int sizeInKB) throws IOException {
        Random rand = new Random();
        try(FSDataOutputStream fos = fs.create(file)) {
            byte[] buffer = new byte[sizeInKB * 1024];
            rand.nextBytes(buffer);
            fos.write(buffer);
            fos.flush();
        }
    }

    @Test
    public void testNonRecursiveDeleteScenarios() throws Exception {
        Path homeDirPath = fs.getHomeDirectory();
        String homeDir = homeDirPath.toString();

        Path testFile = new Path(homeDir + "/TestDir0/TestFile");

        // Create a zero-byte dir and a test file together
        createAndUploadFile(testFile, 1);

        // Try non-recursive delete on a non-empty dir
        boolean dirDeleted = false;
        try {
            dirDeleted = fs.delete(new Path(homeDir + "/TestDir0"), false);
        } catch(IOException e) {
            // Excepted the exception because of non-empty dir
        }
        assertTrue(!dirDeleted);

        // Delete the TestFile to make the zero-byte dir remains.
        fs.delete(testFile, false);

        // Create another zero byte dir TestDir1 in the second level
        testFile = new Path(homeDir + "/TestDir0/TestDir1/TestFile");
        createAndUploadFile(testFile, 1);

        // Make two zero byte dirs one inside other remain
        fs.delete(testFile, false);

        // Even now the directory TestDir0 should not be deleted, as TestDir1 still remains
        try {
            dirDeleted = fs.delete(new Path(homeDir + "/TestDir0"), false);
        } catch(IOException e) {
            // Excepted the exception because of non-empty dir
        }
        assertTrue(!dirDeleted);

        // Try clean up of TestDir1 with non-recursive delete. It should get deleted as it is empty.
        dirDeleted = fs.delete(new Path(homeDir + "/TestDir0/TestDir1"), false);
        assertTrue(dirDeleted);

        dirDeleted = fs.delete(new Path(homeDir + "/TestDir0"), false);
        assertTrue(dirDeleted);
    }

    @Test
    public void testRecursiveDelete() throws Exception {
        Path homeDirPath = fs.getHomeDirectory();
        String homeDir = homeDirPath.toString();

        Path testDirPath = new Path(homeDir + "/Dir1Level0");
        if (fs.exists(testDirPath)) {
            fs.delete(testDirPath, true);
        }

        prepareHierarchy(homeDir);

        fs.delete(testDirPath, true);

        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(homeDirPath, false);
        assertTrue(!iter.hasNext());

        iter = fs.listFiles(homeDirPath, true);
        assertTrue(!iter.hasNext());

        // Check recursive delete of a subdirectory inside the main directory
        createAndUploadFile(new Path(homeDir + "/Dir1Level0/1/2/001"), 1);
        createAndUploadFile(new Path(homeDir + "/Dir1Level0/1/2/002"), 1);
        initNonZeroByteMarkerDirsWithFile(new Path(homeDir + "/Dir1Level0/1/3/001"));
        initNonZeroByteMarkerDirsWithFile(new Path(homeDir + "/Dir1Level0/1/3/002"));
        initNonZeroByteMarkerDirsWithFile(new Path(homeDir + "/Dir1Level0/1/3/4/001"));
        createAndUploadFile(new Path(homeDir + "/Dir1Level0/1/001"), 1);
        createAndUploadFile(new Path(homeDir + "/Dir1Level0/1/5/001"), 1);

        fs.delete(new Path(homeDir + "/Dir1Level0/1/3/4"), true);
        iter = fs.listFiles(new Path(homeDir + "/Dir1Level0/1/3"), true);
        assertTrue(iter.hasNext());

        Set<Path> partialPathSet = new HashSet<>();
        while (iter.hasNext()) {
            partialPathSet.add(iter.next().getPath());
        }
        assertTrue(partialPathSet.contains(new Path(homeDir + "/Dir1Level0/1/3/001")));
        assertTrue(partialPathSet.contains(new Path(homeDir + "/Dir1Level0/1/3/002")));

        fs.delete(new Path(homeDir + "/Dir1Level0/1/2"), true);
        fs.delete(new Path(homeDir + "/Dir1Level0/1/3"), true);

        iter = fs.listFiles(new Path(homeDir + "/Dir1Level0/"), true);

        partialPathSet.clear();
        while (iter.hasNext()) {
            partialPathSet.add(iter.next().getPath());
        }
        assertTrue(partialPathSet.contains(new Path(homeDir + "/Dir1Level0/1/001")));
        assertTrue(partialPathSet.contains(new Path(homeDir + "/Dir1Level0/1/5/001")));

        // Finally cleanup testDirs
        fs.delete(testDirPath, true);
    }

    /*
        Homedir
        ..Dir1Level0
        ....Dir1Level1
        ......Dir1Level2
        ........File1 (Size 1KB)
        ........File2 (Size 1KB)
        ........Dir1Level3 (Empty)
        ......Dir2Level2 (Empty)
        ......Dir3Level2
        ........File1 (Size 1KB)
        ......File1 (Size 1KB)
        ....Dir2Level1 (Empty)
        ....Dir3Level1 created without zero marker
        ......Dir1Level2 created without zero marker
        ........File1 (Size 1KB)
        ......Dir2Level2 created without zero marker (And deleted later by delete file)
        ......File1 (Size 1KB)
        ....Dir4Level1 created without zero marker
        ......Dir1Level2 created without zero marker
        ........File1 (Size 1KB)
        ........Dir1Level3 (Created using mkdirs)
        ..........File1 (Size 1KB)
     */
    @Test
    public void testGetContentSummary() throws Exception {

        Path homeDirPath = fs.getHomeDirectory();
        String homeDir = homeDirPath.toString();

        Path testDirPath = new Path(homeDir + "/Dir1Level0");
        if (fs.exists(testDirPath)) {
            fs.delete(testDirPath, true);
        }

        try {
            prepareHierarchy(homeDir);

            String[] paths = {"/Dir1Level0", "/Dir1Level0/Dir1Level1/Dir1Level2",
                    "/Dir1Level0/Dir1Level1/Dir3Level2", "/Dir1Level0/Dir2Level1", "/Dir1Level0/Dir1Level1",
                    "/Dir1Level0/Dir1Level1/File1", "/Dir1Level0/Dir3Level1/Dir1Level2",
                    "/Dir1Level0/Dir3Level1/Dir1Level2/File1", "/Dir1Level0/Dir3Level1/", "/Dir1Level0/Dir4Level1/",
                    "/Dir1Level0/Dir4Level1/Dir1Level2/Dir1Level3"};
            int[] expectedDirCounts = {12, 2, 1, 1, 5, 0, 1, 0, 2, 3, 1};
            int[] expectedFileCounts = {8, 2, 1, 0, 4, 1, 1, 1, 2, 2, 1};
            int[] expectedLengths = {9216, 3072, 1024 ,0, 5120, 1024, 1024, 1024, 2048, 2048, 1024};

            int count = 0;
            for (String path : paths) {
                ContentSummary contentSummary = fs.getContentSummary(new Path(homeDir + path));
                assertEquals(expectedDirCounts[count], contentSummary.getDirectoryCount());
                assertEquals(expectedFileCounts[count], contentSummary.getFileCount());
                assertEquals(expectedLengths[count], contentSummary.getLength());
                count++;
            }
        } finally {
            fs.delete(testDirPath, true);
        }
    }
}
