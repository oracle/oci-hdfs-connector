package com.oracle.bmc.hdfs;

import org.apache.hadoop.conf.Configuration;

public class GlobalConfigHolder {
    private static Configuration conf;

    public static void setConf(Configuration configuration) {
        conf = configuration;
    }

    public static Configuration getConf() {
        return conf;
    }
}

