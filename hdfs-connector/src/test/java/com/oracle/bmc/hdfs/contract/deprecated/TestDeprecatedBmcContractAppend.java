/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.contract.deprecated;

import com.oracle.bmc.hdfs.IntegrationTestCategory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractAppendTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.junit.experimental.categories.Category;

@Category({IntegrationTestCategory.class})
public class TestDeprecatedBmcContractAppend extends AbstractContractAppendTest {
    @Override
    protected AbstractFSContract createContract(final Configuration conf) {
        return new DeprecatedBmcContract(conf);
    }
}
