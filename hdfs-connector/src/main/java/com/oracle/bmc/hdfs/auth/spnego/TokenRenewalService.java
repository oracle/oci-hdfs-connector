package com.oracle.bmc.hdfs.auth.spnego;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * TokenRenewalService is responsible for managing the renewal of authentication tokens.
 * It schedules automatic token refreshes for registered providers at appropriate intervals.
 */
@Slf4j
public class TokenRenewalService {

    private static TokenRenewalService instance;
    private final Set<UPSTAuthenticationDetailsProvider> providers = Collections.synchronizedSet(new HashSet<>());
    private final ThreadFactory daemonFactory = r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("upst-token-renewal-thread");
        return thread;
    };
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(daemonFactory);

    private TokenRenewalService() { }

    public static synchronized TokenRenewalService getInstance() {
        if (instance == null) {
            instance = new TokenRenewalService();
        }
        return instance;
    }

    public void register(UPSTAuthenticationDetailsProvider provider) {
        providers.add(provider);
        scheduleRefresh(provider);
    }

    private void scheduleRefresh(UPSTAuthenticationDetailsProvider provider) {
        long delay = provider.getTimeUntilRefresh();
        executorService.schedule(() -> {
            provider.refresh();
            scheduleRefresh(provider);
        }, delay, TimeUnit.MILLISECONDS);
    }
}
