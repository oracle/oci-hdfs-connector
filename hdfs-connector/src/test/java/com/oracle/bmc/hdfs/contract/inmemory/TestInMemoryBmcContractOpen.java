package com.oracle.bmc.hdfs.contract.inmemory;

import com.oracle.bmc.hdfs.contract.BmcContract;
import com.oracle.bmc.hdfs.contract.TestBmcContractOpen;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestInMemoryBmcContractOpen extends TestBmcContractOpen {
    @Override
    protected AbstractFSContract createContract(final Configuration conf) {
        return new BmcContract.InMemory(conf);
    }
}
