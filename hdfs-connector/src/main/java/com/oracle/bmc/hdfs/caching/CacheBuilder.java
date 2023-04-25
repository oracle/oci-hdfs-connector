/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.caching;

/**
 * A builder for a {@link Cache}
 * @param <K> key type
 * @param <V> value type
 * @param <B> builder type
 */
public interface CacheBuilder<K, V, B extends CacheBuilder<K, V, B>> {
    /**
     * Set the weigher.
     * @param weigher weigher
     * @return this builder
     */
    B weigher(Cache.Weigher<K, V> weigher);

    /**
     * Set the removal listener.
     * @param removalListener removal listener
     * @return this builder
     */
    B removalListener(Cache.RemovalListener<K, V> removalListener);

    /**
     * Build the {@link Cache}.
     * @return loading cache
     */
    Cache<K, V> build();
}
