package com.oracle.bmc.hdfs.auth.spnego;

import com.oracle.bmc.auth.AuthCachingPolicy;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.RefreshableOnNotAuthenticatedProvider;
import org.apache.hadoop.conf.Configuration;

import java.io.InputStream;

/**
 * A custom authenticator which uses UPST to communicate with Object Storage.
 *
 * To use this authenticator,
 * set {@code fs.oci.client.custom.authenticator} to {@code com.oracle.bmc.hdfs.auth.spnego.UPSTAuthenticationCustomAuthenticator}.
 */
@AuthCachingPolicy(cacheKeyId = false, cachePrivateKey = false)
public class UPSTAuthenticationCustomAuthenticator implements BasicAuthenticationDetailsProvider, RefreshableOnNotAuthenticatedProvider<String> {

    private final UPSTAuthenticationDetailsProvider upstAuthProvider;

    public UPSTAuthenticationCustomAuthenticator(Configuration conf) {
        this.upstAuthProvider = new UPSTAuthenticationDetailsProvider(conf);
    }

    @Override
    public String getKeyId() {
        return "ST$" + upstAuthProvider.getKeyId();
    }

    @Override
    public InputStream getPrivateKey() {
        return upstAuthProvider.getPrivateKey();
    }

    @Override
    public String getPassPhrase() {
        return null;
    }

    @Override
    public char[] getPassphraseCharacters() {
        return upstAuthProvider.getPassphraseCharacters();
    }

    @Override
    public String refresh() {
        return upstAuthProvider.refresh();
    }
}
