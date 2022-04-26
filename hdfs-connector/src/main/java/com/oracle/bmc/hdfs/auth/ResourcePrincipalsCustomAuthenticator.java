/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.auth;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthCachingPolicy;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.RefreshableOnNotAuthenticatedProvider;
import com.oracle.bmc.auth.RegionProvider;
import com.oracle.bmc.auth.ResourcePrincipalAuthenticationDetailsProvider;
import com.oracle.bmc.auth.internal.EnvironmentRptPathProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;

/**
 * A custom authenticator which uses resource principals authentication to communicate with Object Storage.
 * It supports RPv1.1 and RPv2.2.
 *
 * To use this authenticator,
 * set {@code fs.oci.client.custom.authenticator} to {@code com.oracle.bmc.hdfs.auth.ResourcePrincipalsCustomAuthenticator}.
 */
@Slf4j
@AuthCachingPolicy(cacheKeyId = false, cachePrivateKey = false)
public class ResourcePrincipalsCustomAuthenticator
        implements BasicAuthenticationDetailsProvider, RegionProvider,
                RefreshableOnNotAuthenticatedProvider<String> {

    final static String OCI_RESOURCE_PRINCIPAL_VERSION = "OCI_RESOURCE_PRINCIPAL_VERSION";
    private static final String RP_VERSION_1_1 = "1.1";
    private ResourcePrincipalAuthenticationDetailsProvider provider;

    public ResourcePrincipalsCustomAuthenticator(final Configuration conf) {
        final String resourcePrincipalVersion = System.getenv(OCI_RESOURCE_PRINCIPAL_VERSION);
        if (resourcePrincipalVersion == null) {
            throw new IllegalArgumentException(
                    OCI_RESOURCE_PRINCIPAL_VERSION + " environment variable missing");
        }
        LOG.info(
                "ResourcePrincipalsCustomAuthenticator is using resource principal version "
                        + resourcePrincipalVersion);
        if (resourcePrincipalVersion.equals(RP_VERSION_1_1)) {
            provider =
                    ResourcePrincipalAuthenticationDetailsProvider.builder()
                            .resourcePrincipalTokenPathProvider(new EnvironmentRptPathProvider())
                            .build();
        } else {
            provider = ResourcePrincipalAuthenticationDetailsProvider.builder().build();
        }
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
