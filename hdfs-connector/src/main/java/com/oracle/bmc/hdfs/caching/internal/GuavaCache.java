/**
 * Copyright (c) 2016, 2021, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.caching.internal;

import java.util.concurrent.ConcurrentMap;

import com.oracle.bmc.hdfs.caching.Cache;
import lombok.RequiredArgsConstructor;

/**
 * A Guava {@link com.google.common.cache.Cache} implementation of {@link Cache}.
 * @param <K> key type
 * @param <V> value type
 */
@RequiredArgsConstructor
public class GuavaCache<K, V> implements Cache<K, V> {
    private final com.google.common.cache.Cache<K, V> guavaCache;

    @Override
    public void invalidateAll() {
        guavaCache.invalidateAll();
    }

    @Override
    public void invalidate(K key) {
        guavaCache.invalidate(key);
    }

    @Override
    public void cleanUp() {
        guavaCache.cleanUp();
    }

    @Override
    public ConcurrentMap<K, V> asMap() {
        return guavaCache.asMap();
    }

    @Override
    public Statistics getStatistics() {
        return new GuavaCacheStatistics(guavaCache.stats());
    }

    @Override
    public V getIfPresent(K key) {
        return guavaCache.getIfPresent(key);
    }

    @Override
    public void put(K key, V value) {
        guavaCache.put(key, value);
    }
}
