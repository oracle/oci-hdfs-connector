package com.oracle.bmc.hdfs.auth.spnego;

import com.oracle.bmc.auth.AuthCachingPolicy;
import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.auth.RefreshableOnNotAuthenticatedProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
@AuthCachingPolicy(cacheKeyId = false, cachePrivateKey = false)
public class UPSTAuthenticationDetailsProvider implements BasicAuthenticationDetailsProvider, RefreshableOnNotAuthenticatedProvider<String> {

    private final UPSTManager upstManager;
    private volatile UPSTResponse upstResponse;
    private final Object refreshLock = new Object();
    private final Configuration conf;
    private static final SimpleDateFormat SDF = new SimpleDateFormat("E, dd MMM yyyy HH:mm:ss z");
    private static final long TOKEN_REFRESH_ADVANCE_PERIOD = 5 * 60 * 1000;

    public UPSTAuthenticationDetailsProvider(Configuration conf) {
        this(conf, new UPSTManagerFactory(conf));
        init();
    }

    protected UPSTAuthenticationDetailsProvider(Configuration conf, UPSTManagerFactory upstManagerFactory) {
        this.conf = conf;
        this.upstManager = upstManagerFactory.createUPSTManager();
    }

    private void init() {
        refresh();
        TokenRenewalService.getInstance().register(this);
    }

    @Override
    public String getKeyId() {
        String keyId = this.upstResponse.getUpstToken();
        LOG.trace("getKeyId() called. Returning UPST token: {}", keyId);
        return keyId;
    }

    @Override
    public InputStream getPrivateKey() {
        LOG.trace("getPrivateKey() called. Returning private key stream.");
        return new ByteArrayInputStream(this.upstResponse.getPrivateKeyInPEM());
    }

    @Override
    @Deprecated
    public String getPassPhrase() {
        return null;
    }

    @Override
    public char[] getPassphraseCharacters() {
        return new char[0];
    }

    @Override
    public String refresh() {
        synchronized (refreshLock) {
            try {
                String spnegoToken = upstManager.generateSpnegoToken();
                UPSTResponse newResponse = upstManager.getUPSTToken(spnegoToken);
                this.upstResponse = newResponse;
                LOG.info("Successfully refreshed UPST token.");
                return newResponse.getUpstToken();
            } catch (Exception e) {
                LOG.error("Error while refreshing the token", e);
                return null;
            }
        }
    }

    public String getSessionExp() {
        return this.upstResponse.getSessionExp();
    }

    public long getTimeUntilRefresh() {
        try {
            Date sessionExpDate = SDF.parse(getSessionExp());
            return calculateTimeUntilRefresh(sessionExpDate);
        } catch (Exception e) {
            LOG.error("Error calculating time until refresh", e);
            return 0;
        }
    }

    private long calculateTimeUntilRefresh(Date sessionExpDate) {
        long sessionExpMillis = sessionExpDate.getTime();
        long now = System.currentTimeMillis();
        // Refresh the token 5 minutes before it expires to ensure continuous validity
        return sessionExpMillis - now - TOKEN_REFRESH_ADVANCE_PERIOD;
    }
}
