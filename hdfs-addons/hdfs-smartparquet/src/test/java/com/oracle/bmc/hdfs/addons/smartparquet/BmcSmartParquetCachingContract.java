/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.addons.smartparquet;

import com.oracle.bmc.hdfs.contract.BmcContract;
import org.apache.hadoop.conf.Configuration;

public class BmcSmartParquetCachingContract extends BmcContract {
    public static final String CONTRACT_SMART_PARQUET_CACHING =
            "contract/smart-parquet-caching.xml";

    public BmcSmartParquetCachingContract(Configuration conf) {
        super(conf);
        this.addConfResource(CONTRACT_SMART_PARQUET_CACHING);
    }
}
