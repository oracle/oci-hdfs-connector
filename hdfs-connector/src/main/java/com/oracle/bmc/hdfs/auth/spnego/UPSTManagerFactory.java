package com.oracle.bmc.hdfs.auth.spnego;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory class for creating UPSTManager instances.
 */
public class UPSTManagerFactory {

    private final Configuration configuration;

    public UPSTManagerFactory(Configuration configuration) {
        this.configuration = configuration;
    }

    public UPSTManager createUPSTManager() {
        UPSTManager.Builder builder = new UPSTManager.Builder()
                .rsaKeyPairGenerator(new RSAKeyPairGenerator())
                .tokenGenerator(new SpnegoGenerator(configuration))
                .tokenExchangeClient(new IAMTokenExchangeClient(configuration));

        return builder.build();
    }
}
