/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
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
 * A custom authenticator which uses instance principals authentication to communicate with Object Storage
 *
 * To use this authenticator,
 * set {@code fs.oci.client.custom.authenticator} to {@code com.oracle.bmc.hdfs.auth.InstancePrincipalsCustomAuthenticator}.
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
