package com.oracle.bmc.hdfs.addons.smartparquet;

import com.oracle.bmc.hdfs.contract.BmcContract;
import org.apache.hadoop.conf.Configuration;

public class BmcSmartParquetCachingContract extends BmcContract {
    public static final String CONTRACT_SMART_PARQUET_CACHING = "contract/smart-parquet-caching.xml";

    public BmcSmartParquetCachingContract(Configuration conf) {
        super(conf);
        this.addConfResource(CONTRACT_SMART_PARQUET_CACHING);
    }
}