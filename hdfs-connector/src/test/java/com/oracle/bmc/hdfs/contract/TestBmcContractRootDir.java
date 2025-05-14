/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.contract;

import java.io.IOException;

import com.oracle.bmc.hdfs.IntegrationTestCategory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.experimental.categories.Category;

@Category({IntegrationTestCategory.class})
public class TestBmcContractRootDir extends AbstractContractRootDirectoryTest {
    @Override
    protected AbstractFSContract createContract(final Configuration conf) {
        return new BmcContract(conf);
    }

    @Override
    public void testSimpleRootListing() throws IOException {
        super.testSimpleRootListing();
    }
}
