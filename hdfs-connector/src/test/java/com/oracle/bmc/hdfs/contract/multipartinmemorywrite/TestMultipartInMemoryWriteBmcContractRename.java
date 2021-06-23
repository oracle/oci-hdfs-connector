package com.oracle.bmc.hdfs.contract.multipartinmemorywrite;

import com.oracle.bmc.hdfs.contract.BmcContract;
import com.oracle.bmc.hdfs.contract.TestBmcContractRename;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestMultipartInMemoryWriteBmcContractRename extends TestBmcContractRename {
    @Override
    protected AbstractFSContract createContract(final Configuration conf) {
        return new BmcContract.MultipartInMemoryWrite(conf);
    }
}
