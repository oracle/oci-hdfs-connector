package com.oracle.bmc.hdfs.contract.apacheclosingstrategy;

import com.oracle.bmc.hdfs.contract.BmcContract;
import com.oracle.bmc.hdfs.contract.TestBmcContractRootDir;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestApacheClosingStrategyBmcContractRootDir extends TestBmcContractRootDir {
    @Override
    protected AbstractFSContract createContract(final Configuration conf) {
        return new BmcContract.ApacheClosingStrategy(conf);
    }
}
