/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.caching.internal;

import java.time.Duration;
import java.util.Objects;

import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.Weigher;
import com.oracle.bmc.hdfs.caching.Cache;
import com.oracle.bmc.hdfs.caching.CacheBuilderWithWeight;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Default supplier of a new {@link Cache}, which uses a spec string to describe the cache settings.
 * @param <K> key type
 * @param <V> value type
 */
@Slf4j
public class GuavaCacheBuilder<K, V>
        implements CacheBuilderWithWeight<K, V, GuavaCacheBuilder<K, V>> {

    /**
     * Sets the initial capacity of the cache.
     */
    private int initialCapacity = 16;

    /**
     * Whether to record statistics. This incurs additional overhead.
     */
    private boolean recordStats = false;

    /**
     * If set, the maximum size, in number of cached items.
     *
     * Cannot be used together with maximumWeightInBytes.
     */
    private Integer maximumSize = null;

    /**
     * If set, the maximum weight, in bytes per cache section.
     *
     * Cannot be used together with maximumSize.
     */
    private Long maximumWeightInBytes = null;

    /**
     * Sets the number of cache sections.
     */
    private Integer concurrencyLevel = 1;

    /**
     * Sets the duration following the last access of any kind after which an item should be evicted.
     */
    private Duration expireAfterAccess = null;

    /**
     * Sets the duration following the last write of any kind after which an item should be evicted.
     */
    private Duration expireAfterWrite = null;

    private Cache.Weigher<K, V> weigher;
    private Cache.RemovalListener<K, V> removalListener;

    public GuavaCacheBuilder<K, V> initialCapacity(Integer initialCapacity) {
        this.initialCapacity = initialCapacity;
        return this;
    }

    public GuavaCacheBuilder<K, V> recordStats(boolean recordStats) {
        this.recordStats = recordStats;
        return this;
    }

    public GuavaCacheBuilder<K, V> maximumSize(Integer maximumSize) {
        this.maximumSize = maximumSize;
        return this;
    }

    @Override
    public GuavaCacheBuilder<K, V> maximumWeight(Long maximumWeightInBytes) {
        this.maximumWeightInBytes = maximumWeightInBytes;
        return this;
    }

    @Override
    public Long getMaximumWeight() {
        return maximumWeightInBytes;
    }

    public GuavaCacheBuilder<K, V> concurrencyLevel(Integer concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
        return this;
    }

    public GuavaCacheBuilder<K, V> expireAfterAccess(Duration expireAfterAccess) {
        this.expireAfterAccess = expireAfterAccess;
        return this;
    }

    public GuavaCacheBuilder<K, V> expireAfterWrite(Duration expireAfterWrite) {
        this.expireAfterWrite = expireAfterWrite;
        return this;
    }

    @Override
    public GuavaCacheBuilder<K, V> weigher(Cache.Weigher<K, V> weigher) {
        this.weigher = weigher;
        return this;
    }

    @Override
    public GuavaCacheBuilder<K, V> removalListener(Cache.RemovalListener<K, V> removalListener) {
        this.removalListener = removalListener;
        return this;
    }

    @Override
    public GuavaCache<K, V> build() {
        Objects.requireNonNull(removalListener, "removalListener may not be null");

        com.google.common.cache.CacheBuilder guavaCacheBuilder =
                com.google.common.cache.CacheBuilder.newBuilder()
                        .initialCapacity(initialCapacity)
                        .concurrencyLevel(concurrencyLevel)
                        .removalListener(
                                (RemovalListener<K, V>)
                                        notification ->
                                                removalListener.onRemoval(
                                                        new GuavaRemovalNotification<K, V>(
                                                                notification.getKey(),
                                                                notification.getValue(),
                                                                notification.getCause())));

        if (maximumSize != null) {
            guavaCacheBuilder = guavaCacheBuilder.maximumSize(maximumSize);
        }
        if (maximumWeightInBytes != null) {
            guavaCacheBuilder = guavaCacheBuilder.maximumWeight(maximumWeightInBytes);
        }
        if (weigher != null && maximumSize == null) {
            guavaCacheBuilder =
                    guavaCacheBuilder.weigher(
                            (Weigher<K, V>) (key, value) -> weigher.weigh(key, value));
        }
        if (recordStats) {
            guavaCacheBuilder = guavaCacheBuilder.recordStats();
        }
        if (expireAfterAccess != null) {
            guavaCacheBuilder = guavaCacheBuilder.expireAfterAccess(expireAfterAccess);
        }
        if (expireAfterWrite != null) {
            guavaCacheBuilder = guavaCacheBuilder.expireAfterWrite(expireAfterWrite);
        }

        LOG.debug("Creating cache with spec {}", guavaCacheBuilder);

        com.google.common.cache.Cache<K, V> guavaCache = guavaCacheBuilder.build();

        return new GuavaCache<>(guavaCache);
    }

    /**
     * Guava removal notification.
     * @param <KK> key type
     * @param <VV> value type
     */
    public static class GuavaRemovalNotification<KK, VV> extends Cache.RemovalNotification<KK, VV> {
        @Getter private final RemovalCause cause;

        public GuavaRemovalNotification(KK key, VV value, RemovalCause cause) {
            super(key, value);
            this.cause = cause;
        }

        @Override
        public String toString() {
            return "GuavaRemovalNotification{key="
                    + getKey()
                    + ", value="
                    + getValue()
                    + ", cause="
                    + cause
                    + "}";
        }
    }
}
