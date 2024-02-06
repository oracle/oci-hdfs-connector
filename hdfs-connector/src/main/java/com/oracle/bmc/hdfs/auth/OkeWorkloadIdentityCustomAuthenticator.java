package com.oracle.bmc.hdfs.auth;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AuthCachingPolicy;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.RefreshableOnNotAuthenticatedProvider;
import com.oracle.bmc.auth.RegionProvider;
import com.oracle.bmc.auth.okeworkloadidentity.OkeWorkloadIdentityAuthenticationDetailsProvider;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * A custom authenticator which uses OKE Workload Identity authentication to communicate with Object Storage.
 *
 * To use this authenticator,
 * set {@code fs.oci.client.custom.authenticator} to {@code com.oracle.bmc.hdfs.auth.OkeWorkloadIdentityCustomAuthenticator}.
 *
 * @author Brian Parks
 */
@AuthCachingPolicy(cacheKeyId = false, cachePrivateKey = false)
public class OkeWorkloadIdentityCustomAuthenticator
        implements BasicAuthenticationDetailsProvider,
        RegionProvider,
        RefreshableOnNotAuthenticatedProvider<String> {
    private static final Logger LOG = LoggerFactory.getLogger(OkeWorkloadIdentityCustomAuthenticator.class);
    private final OkeWorkloadIdentityAuthenticationDetailsProvider provider;

    /**
     * @param conf {@link Configuration} this is needed to be a valid constructor for a custom authenticator
     */
    public OkeWorkloadIdentityCustomAuthenticator(Configuration conf) {
        LOG.debug("calling builder for OkeWorkloadIdentityAuthenticationDetailsProvider");
        provider = OkeWorkloadIdentityAuthenticationDetailsProvider.builder().build();
        LOG.debug("provider OkeWorkloadIdentityAuthenticationDetailsProvider has been built");
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

    @Override
    public String refresh() {
        return provider.refresh();
    }

    @Override
    public Region getRegion() {
        return provider.getRegion();
    }
}
