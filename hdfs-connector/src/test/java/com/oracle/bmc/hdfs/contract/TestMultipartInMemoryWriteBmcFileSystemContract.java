/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestMultipartInMemoryWriteBmcFileSystemContract extends TestBmcFileSystemContract {

    public static Path INITIAL_WORKING_DIRECTORY;

    @BeforeClass
    public static void setUpStatic() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract.MultipartInMemoryWrite contract = new BmcContract.MultipartInMemoryWrite(configuration);
        contract.init();
        INITIAL_WORKING_DIRECTORY = contract.getTestFileSystem().getWorkingDirectory();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        final Configuration configuration = new Configuration();
        final BmcContract.MultipartInMemoryWrite contract =
                new BmcContract.MultipartInMemoryWrite(configuration);
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
}
