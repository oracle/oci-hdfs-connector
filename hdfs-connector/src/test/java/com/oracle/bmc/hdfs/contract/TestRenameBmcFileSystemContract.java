package com.oracle.bmc.hdfs.contract;

import com.oracle.bmc.hdfs.IntegrationTestCategory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@Category({IntegrationTestCategory.class})
public class TestRenameBmcFileSystemContract extends TestBmcFileSystemContract {
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

    /**
     * Rename foo to bar when foo is a file and bar is a directory and bar/foo is a directory
     */
    @Test
    public void testRenameFileMoveToExistingDirectoryNameConflict() throws Exception {
        assumeTrue(renameSupported());

        Path src = path("testRenameFileMoveToExistingDirectoryNameConflict/foo");
        createFile(src);
        Path dst = path("testRenameFileMoveToExistingDirectoryNameConflictNew/bar/foo");
        fs.mkdirs(dst);
        rename(src, dst.getParent(), false, true, true);
    }

    /**
     * Rename foo to bar when foo is a directory and bar is a directory and /bar/foo is a directory
     */

    @Test
    public void testRenameDirectoryMoveToExistingDirectoryNameConflict() throws Exception {
        assumeTrue(renameSupported());

        Path src = path("testRenameDirectoryMoveToExistingDirectoryNameConflict/foo");
        fs.mkdirs(src);
        Path dst = path("testRenameDirectoryMoveToExistingDirectoryNameConflictNew/bar/foo");
        fs.mkdirs(dst);
        rename(src, dst.getParent(), false, true, true);
    }

    /**
     * Rename foo to bar when foo is a file and bar is a directory and bar/foo is a file
     */
    @Test
    public void testRenameDirectoryMoveToExistingFileNameConflict() throws Exception {
        assumeTrue(renameSupported());

        Path src = path("testRenameDirectoryMoveToExistingFileNameConflict/foo");
        fs.mkdirs(src);
        Path dst = path("testRenameDirectoryMoveToExistingFileNameConflictNew/bar/foo");
        createFile(dst);
        rename(src, dst.getParent(), false, true, true);
    }

    /**
     * Rename foo to bar when foo is a directory and bar is a directory and bar/foo does not exist
     */
    @Test
    public void testRenameDirectoryMoveToExistingDirectoryNoNameConflict() throws Exception {
        assumeTrue(renameSupported());

        Path src = path("testRenameDirectoryMoveToExistingDirectoryNoNameConflict/foo");
        fs.mkdirs(src);
        Path dst = path("testRenameDirectoryMoveToExistingDirectoryNoNameConflictNew/bar");
        fs.mkdirs(dst);
        rename(src, dst, true, false, true);
    }


    /**
     * Rename foo to bar when foo is a directory and bar is a directory and bar/foo is a
     * directory that doesn't contain any files that conflict with those in foo
     */
    @Test
    public void testRenameDirectoryMoveToExistingDirectorySameName() throws Exception {
        assumeTrue(renameSupported());

        final Path src = path("testRenameDirectoryMoveToExistingDirectorySameName/foo");
        createFile(path(src + "/baz"));
        Path dst = path("testRenameDirectoryMoveToExistingDirectorySameNameNew/bar");
        createFile(path(dst + "/foo/qux"));
        rename(src, dst, false, true, true);
    }

    /**
     * Rename foo to bar when foo is a directory and bar is a directory and bar/foo is a
     * directory that doesn't contain any files that conflict with those in foo
     *
     * Possible results:
     * 1, testRename/baz/ testRename/bar/
     * 2, testRename/foo/, testRename/baz/
     * 3, testRename/baz/, testRename/baz/foo/
     * 4, testRename/baz/, testRename/baz/bar/
     */
    @Test
    public void testRenameDirectoryMoveToNonExistingSameDirectoryConcurrently() throws Exception {
        assumeTrue(renameSupported());
        Path src1 = path("testRename/foo");
        fs.mkdirs(src1);
        Path src2 = path("testRename/bar");
        fs.mkdirs(src2);

        Path[] paths = {src1, src2};
        ExecutorService executorService = Executors.newFixedThreadPool(paths.length);
        CountDownLatch countDownLatch = new CountDownLatch(paths.length);
        for (Path p : paths) {
            executorService.submit(() -> {
                countDownLatch.countDown();
                try {
                    countDownLatch.await();
                    fs.rename(p, path("testRename/baz"));
                } catch (Throwable t) {
                    // Nothing we can do here
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.SECONDS);
        FileStatus status = fs.getFileStatus(path("testRename/baz"));
        assertNotNull(status);
        assertTrue(status.isDirectory());
        assertEquals(0L, status.getLen());
    }
}
