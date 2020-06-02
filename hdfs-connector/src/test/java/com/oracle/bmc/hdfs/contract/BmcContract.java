/**
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
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
