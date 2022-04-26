/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
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
public interface CacheBuilderWithWeight<K, V, B extends CacheBuilderWithWeight<K, V, B>>
        extends CacheBuilder<K, V, B> {

    B maximumWeight(Long maximumWeightInBytes);

    Long getMaximumWeight();
}
