package com.oracle.bmc.hdfs.contract.inmemory;

import com.oracle.bmc.hdfs.contract.BmcContract;
import com.oracle.bmc.hdfs.contract.TestBmcContractMkdir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestInMemoryBmcContractMkdir extends TestBmcContractMkdir {
    @Override
    protected AbstractFSContract createContract(final Configuration conf) {
        return new BmcContract.InMemory(conf);
    }
}
