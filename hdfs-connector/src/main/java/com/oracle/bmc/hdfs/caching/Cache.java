/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.caching;

import java.util.AbstractMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A cache interface.
 * @param <K> key type
 * @param <V> value type
 */
public interface Cache<K, V> {
    /**
     * Get the value for the key if it exists in the cache, otherwise return null.
     * @param key key
     * @return value, or null if it didn't exist in the cache
     */
    V getIfPresent(K key);

    /**
     * Put the value for the key into the cache.
     * @param key key
     * @param value value
     */
    void put(K key, V value);

    /**
     * Invalidate all cache entries.
     */
    void invalidateAll();

    /**
     * Invalidate a single cache entry.
     * @param key key for the entry to be invalidated
     */
    void invalidate(K key);

    /**
     * Run cache clean-up, e.g. to enforce sizes.
     */
    void cleanUp();

    /**
     * Provide a view into the cache as a {@link java.util.Map}.
     * @return map
     */
    ConcurrentMap<K, V> asMap();

    /**
     * Returns statistics, if enabled.
     * @return statistics
     */
    Statistics getStatistics();

    /**
     * Exception was thrown while getting the cache value.
     */
    class UncheckedExecutionException extends RuntimeException {
        public UncheckedExecutionException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Notification of a removal from the cache.
     * @param <KK> key type
     * @param <VV> value type
     */
    class RemovalNotification<KK, VV> extends AbstractMap.SimpleEntry<KK, VV> {
        public RemovalNotification(KK key, VV value) {
            super(key, value);
        }
    }

    /**
     * Listener for removals from the cache.
     * @param <KK> key type
     * @param <VV> value type
     */
    interface RemovalListener<KK, VV> {
        /**
         * Notifies the listener that a removal occurred at some point in the past.
         * @param notification notification object
         */
        void onRemoval(RemovalNotification<KK, VV> notification);
    }

    /**
     * Loader to get the value for the key into the cache.
     * @param <KK> key type
     * @param <VV> value type
     */
    interface Loader<KK, VV> {
        /**
         * Load the value for the key.
         * @param key key
         * @return value
         * @throws Exception any exception thrown during loading of the value
         */
        VV load(KK key) throws Exception;
    }

    /**
     * Weigher for determining the weight of an entry.
     * @param <KK> key type
     * @param <VV> value type
     */
    interface Weigher<KK, VV> {
        /**
         * Determine the weight of the entry
         * @param key entry key
         * @param value entry value
         * @return weight
         */
        int weigh(KK key, VV value);
    }

    /**
     * Interface for providing cache statistics, if enabled.
     */
    interface Statistics {
        long requestCount();

        long hitCount();

        double hitRate();

        long missCount();

        double missRate();

        long evictionCount();
    }
}
