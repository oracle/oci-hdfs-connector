package com.oracle.bmc.hdfs.contract.multipartinmemorywrite;

import com.oracle.bmc.hdfs.contract.BmcContract;
import com.oracle.bmc.hdfs.contract.TestBmcContractMkdir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestMultipartInMemoryWriteBmcContractMkdir extends TestBmcContractMkdir {
    @Override
    protected AbstractFSContract createContract(final Configuration conf) {
        return new BmcContract.MultipartInMemoryWrite(conf);
    }
}
