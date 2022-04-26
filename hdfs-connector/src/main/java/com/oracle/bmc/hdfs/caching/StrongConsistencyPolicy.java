/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.caching;

import javax.ws.rs.core.Response;

import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorage;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.transfer.DownloadManager;
import lombok.extern.slf4j.Slf4j;

/**
 * This consistency policy evicts cache entries that have an etag that differs from the current etag.
 *
 * Note that this incurs the overhead of making a HeadObject request before every GetObject request. After that
 * request, any cached items with differing etags will be evicted, forcing the object to be read again from Object
 * Storage.
 *
 * The synchronization point is the point at which the HeadObject request is processed by Object Storage.
 * The etag becomes part of the cache key, meaning two requests for the same object can proceed in parallel if the
 * etag from the HeadObject request differed.
 *
 * This means that if an object is requested, and the download is ongoing, then the object is changed, and
 * a second request is made, both requests will continue and deliver differing results. It is irrelevant when those
 * requests finish; the synchronization point is when they were begun (or more accurately, when the HeadObject
 * request was processed).
 */
@Slf4j
public class StrongConsistencyPolicy implements ConsistencyPolicy {
    public StrongConsistencyPolicy() {}

    @Override
    public CachingObjectStorage.GetObjectRequestCacheKey constructKey(GetObjectRequest request) {
        return new CachingObjectStorage.GetObjectRequestCacheKey(
                GetObjectRequest.builder()
                        .copy(request)
                        .opcClientRequestId(null)
                        .retryConfiguration(null)
                        .invocationCallback(null)
                        .ifMatch(null)
                        .build());
    }

    @Override
    public GetObjectResponse initiateRequest(
            ObjectStorage client,
            DownloadManager downloadManager,
            GetObjectRequest request,
            CachingObjectStorage.GetObjectRequestCacheKey key,
            CachingObjectStorage.GetObjectResponseCacheValue previousValue) {
        if (request.getIfMatch() == null) {
            return withoutIfMatch(client, downloadManager, request, key, previousValue);
        } else {
            return withIfMatch(client, downloadManager, request, key, previousValue);
        }
    }

    private GetObjectResponse withoutIfMatch(
            ObjectStorage client,
            DownloadManager downloadManager,
            GetObjectRequest request,
            CachingObjectStorage.GetObjectRequestCacheKey key,
            CachingObjectStorage.GetObjectResponseCacheValue previousValue) {
        // we want strong consistency, so we do a GetObject request with if-none-match=<etag>
        GetObjectRequest.Builder builder = GetObjectRequest.builder().copy(request);

        String previousEtag = null;
        if (previousValue != null) {
            previousEtag = previousValue.getOriginalResponse().getETag();
        }

        builder = CachingObjectStorage.Handler.updateRequestId(builder, request);

        GetObjectResponse object;
        if (previousEtag != null) {
            LOG.debug(
                    "Making get request with if-none-match='{}' check for key='{}'",
                    previousEtag,
                    key);

            builder = builder.ifNoneMatch(previousEtag);

            // we can't use the download manager here, because it would set both if-none-match and if-match
            // causing a 400; that means if the object changed, then we have to download everything in a
            // single thread
            object = client.getObject(builder.build());
        } else {
            LOG.debug("Making new get request (without if-none-match check) for key='{}'", key);
            object = downloadManager.getObject(builder.build());
        }

        LOG.debug(
                "Service returned '{}-{}' for key='{}', ETag is '{}', body is '{}'",
                object.get__httpStatusCode__(),
                Response.Status.fromStatusCode(object.get__httpStatusCode__()).getReasonPhrase(),
                key,
                object.getETag(),
                object.getInputStream());
        if (object.get__httpStatusCode__() == Response.Status.NOT_MODIFIED.getStatusCode()) {
            return null;
        }
        return object;
    }

    private GetObjectResponse withIfMatch(
            ObjectStorage client,
            DownloadManager downloadManager,
            GetObjectRequest request,
            CachingObjectStorage.GetObjectRequestCacheKey key,
            CachingObjectStorage.GetObjectResponseCacheValue previousValue) {
        String previousEtag = null;
        if (previousValue != null) {
            previousEtag = previousValue.getOriginalResponse().getETag();
        }

        if (previousEtag == null) {
            // value not cached before, make the request
            LOG.trace("New previously uncached request for '{}'", key);

            GetObjectRequest.Builder builder = GetObjectRequest.builder().copy(request);

            builder = CachingObjectStorage.Handler.updateRequestId(builder, request);

            // it could throw a 412 - Precondition Failed here because of the if-match,
            // but in that case, the right thing to do is to throw anyway, so just
            // make the request and don't catch the exception
            GetObjectResponse getObjectResponse = downloadManager.getObject(builder.build());

            // didn't throw, this value is new
            return getObjectResponse;
        }

        // this value has been cached before
        if (request.getIfMatch().equals(previousEtag)) {
            // and the cached value has the etag the request is asking for in the if-match header;
            // do a really short request to check if that is also the still the etag on the server is
            LOG.trace("Previously cached request with same etag '{}' for '{}'", previousEtag, key);

            GetObjectRequest.Builder builder =
                    GetObjectRequest.builder()
                            .copy(request)
                            .range(
                                    new Range(
                                            0L,
                                            0L)); // doing this is easier than a HeadObject request

            builder = CachingObjectStorage.Handler.updateRequestId(builder, request);

            // it could throw a 412 - Precondition Failed here because of the if-match,
            // but in that case, the right thing to do is to throw anyway, so just
            // make the request and don't catch the exception
            GetObjectResponse shortRangeResult = client.getObject(builder.build());

            LOG.trace("Etag is still '{}' for '{}', using cached data", previousEtag, key);

            // didn't throw, so use the cached value
            return null;
        }

        // this value has been cached, but the cached etag is different than what the request is asking for
        LOG.trace(
                "Request with if-match '{}' for previously cached request with different etag '{}' for '{}'",
                request.getIfMatch(),
                previousEtag,
                key);

        GetObjectRequest.Builder builder = GetObjectRequest.builder().copy(request);

        // it could throw a 412 - Precondition Failed here because of the if-match,
        // but in that case, the right thing to do is to throw anyway, so just
        // make the request and don't catch the exception
        GetObjectResponse getObjectResponse = downloadManager.getObject(builder.build());

        LOG.trace(
                "Etag matched if-match in request '{}', not previous etag '{}', updating cache for '{}'",
                request.getIfMatch(),
                previousEtag,
                key);

        return getObjectResponse;
    }
}
