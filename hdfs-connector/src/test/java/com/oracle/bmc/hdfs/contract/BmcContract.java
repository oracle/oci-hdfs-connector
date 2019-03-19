/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

public class BmcContract extends AbstractBondedFSContract {
    public static final String CONTRACT_XML = "contract/oci.xml";
    public static final String CREDENTIALS_XML = "contract/oci-credentials.xml";

    public BmcContract(final Configuration conf) {
        super(conf);
        this.addConfResource(CONTRACT_XML);
        this.addConfResource(CREDENTIALS_XML);
    }

    @Override
    public String getScheme() {
        return "oci";
    }
}
