package com.oracle.bmc.hdfs.auth;


import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthCachingPolicy;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.RefreshableOnNotAuthenticatedProvider;
import com.oracle.bmc.auth.RegionProvider;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.auth.internal.EnvironmentRptPathProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;

/**
 * A custom authenticator which uses resource principals authentication to communicate with Object Storage.
 * It supports RPv1.1 and RPv2.2.
 *
 * To use this authenticator,
 * set {@code fs.oci.client.custom.authenticator} to {@code com.oracle.bmc.hdfs.auth.ResourcePrincipalsCustomAuthenticator}.
 */
@AuthCachingPolicy(cacheKeyId = false, cachePrivateKey = false)
public class ResourcePrincipalsCustomAuthenticator implements BasicAuthenticationDetailsProvider,
        RegionProvider, RefreshableOnNotAuthenticatedProvider<String> {

    private ResourcePrincipalAuthenticationDetailsProvider provider;

    public ResourcePrincipalsCustomAuthenticator(final Configuration conf) {
        provider = ResourcePrincipalAuthenticationDetailsProvider.builder()
                .resourcePrincipalTokenPathProvider(new EnvironmentRptPathProvider())
                .build();
    }

    @Override
    public String refresh() {
        return provider.refresh();
    }

    @Override
    public Region getRegion() {
        return provider.getRegion();
    }

    @Override
    public String getKeyId() {
        return provider.getKeyId();
    }

    @Override
    public InputStream getPrivateKey() {
        return provider.getPrivateKey();
    }

    @Override
    public String getPassPhrase() {
        return provider.getPassPhrase();
    }

    @Override
    public char[] getPassphraseCharacters() {
        return provider.getPassphraseCharacters();
    }
}
