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
import lombok.extern.slf4j.Slf4j;

/**
 * This consistency policy does not do any evictions.
 *
 * Using this consistencyPolicy, the cache has no knowledge of etag and if-match/if-none-match` built in and will happily
 * serve contradictory results.
 *
 * Example:
 * 1. Let's say you do a GetObject with if-match=foo, and it succeeds. It will cache that response.
 * 2. Then you do a GetObject with if-match=bar, and because that exact request is not in the cache,
 *    it will go to the service. Let's say that succeeds too. It will cache that response.
 * 3. Now you could make GetObject requests both for if-match=foo and if-match=bar`, and it will serve
 *    successfully from the cache.
 *
 * The fact that if-match=bar succeeded does not evict the request with if-match=foo. Nor do any successful
 * requests with different etag response headers evict them.
 *
 * This policy does not perform any consistency-preserving operations. The cache will retain any object, for as long
 * as the cache eviction policy will allow. If an object changes in Object Storage, that will go unnoticed until the
 * cache evicts a cache entry for time- or size reasons.
 *
 * The cache may even cache and return contradictory responses, e.g. two responses that specify contradictory
 * "if-match" arguments, essentially implying that the object exists with two different etags, and therefore
 * contents, simultaneously.
 *
 * This policy should really only be used if the data in Object Storage does not change, and therefore, consistency
 * does not have to be preserved.
 */
@Slf4j
public class NoOpConsistencyPolicy implements ConsistencyPolicy {
    public NoOpConsistencyPolicy() {}

    @Override
    public CachingObjectStorage.GetObjectRequestCacheKey constructKey(GetObjectRequest request) {
        return new CachingObjectStorage.GetObjectRequestCacheKey(
                GetObjectRequest.builder()
                        .copy(request)
                        .opcClientRequestId(null)
                        .retryConfiguration(null)
                        .invocationCallback(null)
                        .build());
    }

    @Override
    public GetObjectResponse initiateRequest(
            ObjectStorage client,
            DownloadManager downloadManager,
            GetObjectRequest request,
            CachingObjectStorage.GetObjectRequestCacheKey key,
            CachingObjectStorage.GetObjectResponseCacheValue previousValue) {
        // if we have a previous ETag, it means we had something in the cache
        if (previousValue != null) {
            LOG.debug(
                    "Reusing cached request with ETag='{}' check for key '{}'", previousValue, key);
            return null;
        }

        GetObjectRequest.Builder builder = GetObjectRequest.builder().copy(request);
        builder = CachingObjectStorage.Handler.updateRequestId(builder, request);

        LOG.debug("Making new getObject request for key '{}'", key);

        return downloadManager.getObject(builder.build());
    }
}
