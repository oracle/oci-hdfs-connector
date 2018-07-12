/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs.auth;

import java.io.InputStream;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthCachingPolicy;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.auth.RefreshableOnNotAuthenticatedProvider;
import com.oracle.bmc.auth.RegionProvider;

import org.apache.hadoop.conf.Configuration;

/**
 * A custom authenticator which uses instance principals authentication to communicate with
 * Object Storage
 */
@AuthCachingPolicy(cacheKeyId = false, cachePrivateKey = false)
public class InstancePrincipalsCustomAuthenticator
        implements BasicAuthenticationDetailsProvider, RegionProvider,
                RefreshableOnNotAuthenticatedProvider<String> {

    private InstancePrincipalsAuthenticationDetailsProvider provider;

    public InstancePrincipalsCustomAuthenticator(final Configuration conf) {
        provider = InstancePrincipalsAuthenticationDetailsProvider.builder().build();
    }

    @Override
    public String getKeyId() {
        return provider.getKeyId();
    }

    @Override
    public InputStream getPrivateKey() {
        return provider.getPrivateKey();
    }

    @Deprecated
    @Override
    public String getPassPhrase() {
        return provider.getPassPhrase();
    }

    @Override
    public char[] getPassphraseCharacters() {
        return provider.getPassphraseCharacters();
    }

    @Override
    public Region getRegion() {
        return provider.getRegion();
    }

    @Override
    public String refresh() {
        return provider.refresh();
    }
}
