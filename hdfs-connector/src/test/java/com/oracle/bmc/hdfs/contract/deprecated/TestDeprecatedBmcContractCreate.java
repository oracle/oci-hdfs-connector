/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.contract.deprecated;

import com.oracle.bmc.hdfs.IntegrationTestCategory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.fs.contract.ContractTestUtils.skip;

@Category({IntegrationTestCategory.class})
public class TestDeprecatedBmcContractCreate extends AbstractContractCreateTest {
    @Override
    protected AbstractFSContract createContract(final Configuration conf) {
        return new DeprecatedBmcContract(conf);
    }

    // same as the parent class test, except it closes the output stream before testing
    // that the path exists -- object store implementations typically create-on-close,
    // so this just moves the check after the try/finally.
    @Test
    @Override
    public void testCreatedFileIsImmediatelyVisible() throws Throwable {
        this.describe("verify that a newly created file exists as soon as open returns");
        final Path path = this.path("testCreatedFileIsImmediatelyVisible");
        FSDataOutputStream out = null;
        try {
            out = this.getFileSystem().create(path, false, 4096, (short) 1, 1024);
        } finally {
            IOUtils.closeStream(out);
        }

        if (!this.getFileSystem().exists(path)) {
            if (this.isSupported(IS_BLOBSTORE)) {
                // object store: downgrade to a skip so that the failure is visible
                // in test results
                skip(
                        "Filesystem is an object store and newly created files are not immediately visible");
            }
            this.assertPathExists("expected path to be visible before anything written", path);
        }
    }
}
