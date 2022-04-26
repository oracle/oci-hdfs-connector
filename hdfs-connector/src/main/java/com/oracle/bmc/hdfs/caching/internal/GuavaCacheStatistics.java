/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.caching.internal;

import com.google.common.cache.CacheStats;
import com.oracle.bmc.hdfs.caching.Cache;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Guava's cache statistics
 */
@RequiredArgsConstructor
public class GuavaCacheStatistics implements Cache.Statistics {
    private final CacheStats guavaStats;

    @Override
    public long requestCount() {
        return guavaStats.requestCount();
    }

    @Override
    public long hitCount() {
        return guavaStats.hitCount();
    }

    @Override
    public double hitRate() {
        return guavaStats.hitRate();
    }

    @Override
    public long missCount() {
        return guavaStats.missCount();
    }

    @Override
    public double missRate() {
        return guavaStats.missRate();
    }

    @Override
    public long evictionCount() {
        return guavaStats.evictionCount();
    }

    @Override
    public int hashCode() {
        return guavaStats.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object object) {
        return guavaStats.equals(object);
    }

    @Override
    public String toString() {
        return guavaStats.toString();
    }
}
