/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.caching;

import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.transfer.DownloadManager;

/**
 * This interface controls how conflicting items are evicted.
 *
 * Implementations of this class should have a no-argument constructor.
 */
public interface ConsistencyPolicy {
    /**
     * Construct a cache key from the {@link GetObjectRequest}. Set everything that should be ignored when
     * it comes to the keys to null.
     *
     * @param request request
     * @return key
     */
    CachingObjectStorage.GetObjectRequestCacheKey constructKey(GetObjectRequest request);

    /**
     * Initiate the request to get the object.
     *
     * This can have three different outcomes:
     * 1. Either a {@link GetObjectResponse} for data that should be put in the cache and returned; or
     * 2. {@code null} if the data already in the cache should be used; or
     * 3. An exception, which should be bubbled up
     *
     * @param client {@link ObjectStorage} client to use (uncached)
     * @param downloadManager {@link DownloadManager} to use for downloading}
     * @param request request to be made
     * @param key cache key
     * @param previousValue previous value or null
     * @return GetObjectResponse or null or an exception
     */
    GetObjectResponse initiateRequest(
            ObjectStorage client,
            DownloadManager downloadManager,
            GetObjectRequest request,
            CachingObjectStorage.GetObjectRequestCacheKey key,
            CachingObjectStorage.GetObjectResponseCacheValue previousValue);
}
