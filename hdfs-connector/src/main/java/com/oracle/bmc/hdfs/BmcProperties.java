/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs;

import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.hdfs.caching.StrongConsistencyPolicy;
import com.oracle.bmc.objectstorage.ObjectStorage;

import com.oracle.bmc.ClientConfiguration;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.lang.Deprecated;
import java.util.concurrent.TimeUnit;

import static com.oracle.bmc.hdfs.BmcConstants.*;

/**
 * Enum to encapsulate all of the configuration options available to users.
 */
@RequiredArgsConstructor
public enum BmcProperties {
    /**
     * (long, optional) HDFS file block size, in mebibytes. See {@link BmcConstants#BLOCK_SIZE_IN_MB_KEY} for config key
     * name. Default is 32.
     * <p>
     * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html#Data_Blocks
     */
    BLOCK_SIZE_IN_MB(BLOCK_SIZE_IN_MB_KEY, 32L),
    /**
     * (boolean, optional) Flag to enable writing all files to memory (instead of a temp file) before uploading to
     * Object Store. See {@link BmcConstants#IN_MEMORY_WRITE_BUFFER_KEY} for config key name. Default is false.
     *
     * Cannot be enabled at the same time as {@link BmcProperties#MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED}.
     */
    IN_MEMORY_WRITE_BUFFER(IN_MEMORY_WRITE_BUFFER_KEY, false),
    /**
     * (boolean, optional) Flag to enable reading all files to memory first (instead streaming from Object Store), to
     * allow faster seeking. See {@link BmcConstants#IN_MEMORY_READ_BUFFER_KEY} for config key name. Default is false.
     * Incompatible with READ_AHEAD.
     */
    IN_MEMORY_READ_BUFFER(IN_MEMORY_READ_BUFFER_KEY, false),
    /**
     * (string, optional) Fully qualified class name of a custom implementation of {@link ObjectStorage}. See
     * {@link BmcConstants#OBJECT_STORE_CLIENT_CLASS_KEY} for config key name.
     */
    OBJECT_STORE_CLIENT_CLASS(OBJECT_STORE_CLIENT_CLASS_KEY, null),
    /**
     * (string, optional) Fully qualified class name of a custom implementation of
     * {@link BasicAuthenticationDetailsProvider}. See {@link BmcConstants#OBJECT_STORE_AUTHENTICATOR_CLASS_KEY} for config
     * key name.
     */
    OBJECT_STORE_AUTHENTICATOR_CLASS(OBJECT_STORE_AUTHENTICATOR_CLASS_KEY, null),
    /**
     * (string, required) Object Store endpoint name (http://server.path.com) . Note, if a custom client class is used,
     * then this field is not required. See {@link BmcConstants#HOST_NAME_KEY} for config key name.
     */
    HOST_NAME(HOST_NAME_KEY, null),
    /**
     * (string, required) The tenant ID used for authentication. Note, if a custom authenticator class is used, then
     * this field is not required. See {@link BmcConstants#TENANT_ID_KEY} for config key name.
     */
    TENANT_ID(TENANT_ID_KEY, null),
    /**
     * (string, required) The user ID used for authentication. Note, if a custom authenticator class is used, then this
     * field is not required. See {@link BmcConstants#USER_ID_KEY} for config key name.
     */
    USER_ID(USER_ID_KEY, null),
    /**
     * (string, required) The fingerprint used for authentication. Note, if a custom authenticator class is used, then
     * this field is not required. See {@link BmcConstants#FINGERPRINT_KEY} for config key name.
     */
    FINGERPRINT(FINGERPRINT_KEY, null),
    /**
     * (string, required) Path to the PEM file used for authentication. The path is for the local filesystem. Note, if a
     * authenticator client class is used, then this field is not required. See {@link BmcConstants#PEM_FILE_PATH_KEY}
     * for config key name.
     */
    PEM_FILE_PATH(PEM_FILE_PATH_KEY, null),
    /**
     * (string, optional) Pass phrase to use for the PEM file if it's encrypted. See
     * {@link BmcConstants#PASS_PHRASE_KEY} for config key name.
     */
    PASS_PHRASE(PASS_PHRASE_KEY, null),
    /**
     * (string, optional) The region code or region identifier used to establish Object Storage endpoint name.
     * Note, if "fs.oci.client.hostname property" is set, then this field is not required.
     * See {@link BmcConstants#REGION_CODE_OR_ID_KEY} for config key name.
     */
    REGION_CODE_OR_ID(REGION_CODE_OR_ID_KEY, null),
    /**
     * (int, optional) The max time to wait for a connection, in millis. See
     * {@link BmcConstants#CONNECTION_TIMEOUT_MILLIS_KEY} for config key name. Defaults to Java SDK timeout, see
     * {@link ClientConfiguration}.
     */
    CONNECTION_TIMEOUT_MILLIS(CONNECTION_TIMEOUT_MILLIS_KEY, null),
    /**
     * (int, optional) The max time to wait for data, in millis. See {@link BmcConstants#READ_TIMEOUT_MILLIS_KEY} for
     * config key name. Defaults to Java SDK timeout, see {@link ClientConfiguration}.
     */
    READ_TIMEOUT_MILLIS(READ_TIMEOUT_MILLIS_KEY, null),
    /**
     * (boolean, optional) Flag for whether or not multi-part uploads are allowed at all. See
     * {@link BmcConstants#MULTIPART_ALLOWED_KEY} for config key name. Default true.
     */
    MULTIPART_ALLOWED(MULTIPART_ALLOWED_KEY, true),
    /**
     * (int, optional) The number of threads to use for parallel multi-part uploads. Note, any value less than or equal
     * to 0 is interpreted as using the default Java SDK value. A value of 1 means do not use parallel uploads (you can
     * still use multi-part if the "allowed" property is true, but each part will be uploaded serially. This will be
     * one thread per upload). Otherwise, this represents the total number of threads shared amongst all requests (not
     * per request). See {@link BmcConstants#MULTIPART_NUM_UPLOAD_THREADS_KEY} for config key name.
     */
    MULTIPART_NUM_UPLOAD_THREADS(MULTIPART_NUM_UPLOAD_THREADS_KEY, null),
    /**
     * (int, optional) The number of threads to use for parallel MD5 calculation. Note, any value less than or equal to
     * 0 is interpreted as using a default value.A value of 1 means do not use parallel calculation (each hash will be
     * calculated serially in a single thread). See {@link BmcConstants#MD5_NUM_THREADS_KEY} for config key name.
     */
    MD5_NUM_THREADS(MD5_NUM_THREADS_KEY, null),
    /**
     * (int, optional) The minimum size, in mebibytes, an object must be before its eligible for multi-part uploads.
     * Note, any value less than or equal to 0 is interpreted as using the default Java SDK value. See
     * {@link BmcConstants#MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB_KEY} for config key name.
     */
    MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB(MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB_KEY, null),
    /**
     * (int, optional) The minimum size, in mebibytes, each part should be uploaded as. Note, any value less than or
     * equal to 0 is interpreted as using the default Java SDK value. See
     * {@link BmcConstants#MULTIPART_MIN_PART_SIZE_IN_MB_KEY} for config key name.
     * @deprecated use {@link #MULTIPART_PART_SIZE_IN_MB} instead
     */
    @Deprecated
    MULTIPART_MIN_PART_SIZE_IN_MB(MULTIPART_MIN_PART_SIZE_IN_MB_KEY, null),
    /**
     * (int, optional) The part size, in mebibytes that each part should be uploaded as. Note, any value less than or
     * equal to 0 is interpreted as using the default Java SDK value of 128MiB. See
     * {@link BmcConstants#MULTIPART_PART_SIZE_IN_MB_KEY} for config key name.
     */
    MULTIPART_PART_SIZE_IN_MB(MULTIPART_PART_SIZE_IN_MB_KEY, 128),
    /**
     * (string, optional) The HTTP proxy URI if using a proxy to connect to Object Storage Service.  If configured,
     * then an ObjectStorageClient will be configured to use the ApacheConnector provider add-on in order to configure
     * the proxy for the client.  Note: This buffers all requests (uploads) and can impact memory utiliziation.
     * See {@link BmcConstants#HTTP_PROXY_URI_KEY} for the config key name.
     */
    HTTP_PROXY_URI(HTTP_PROXY_URI_KEY, null),
    /**
     * (string, optional) The username to use for the configured HTTP proxy via {@link #HTTP_PROXY_URI}.
     * See {@link BmcConstants#HTTP_PROXY_USERNAME_KEY} for the config key name.
     */
    HTTP_PROXY_USERNAME(HTTP_PROXY_USERNAME_KEY, null),
    /**
     * (string, optional) The password to use with the associated {@link #HTTP_PROXY_USERNAME}.
     * See {@link BmcConstants#HTTP_PROXY_PASSWORD_KEY} for the config key name.
     */
    HTTP_PROXY_PASSWORD(HTTP_PROXY_PASSWORD_KEY, null),
    /**
     * (long, optional) The timeout to use for exponential delay retries for retryable service errors. This value is
     * ignored if {@link BmcProperties#OBJECT_STORE_CLIENT_CLASS} is specified. See
     * {@link BmcConstants#RETRY_TIMEOUT_IN_SECONDS_KEY} for config key name. Default is 30.
     */
    RETRY_TIMEOUT_IN_SECONDS(RETRY_TIMEOUT_IN_SECONDS_KEY, 30L),
    /**
     * (long, optional) The max sleep for delays in between retries for retryable service errors.
     * If the exponential delay crosses this threshold, then the delay time is reset to initial delay. This value is
     * ignored if {@link BmcProperties#OBJECT_STORE_CLIENT_CLASS} is specified. See
     * {@link BmcConstants#RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS_KEY} for config key name. Default is 0.
     */
    RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS(RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS_KEY, 0L),

    /**
     * (boolean, optional) Flag to enable object metadata caching. See
     * {@link BmcConstants#OBJECT_METADATA_CACHING_ENABLED_KEY} for config key name. Default is false.
     *
     * Note: If this is enabled, the OCI HDFS Connector will cache object metadata, and queries will not necessarily
     * reflect the actual data in Object Storage.
     */
    OBJECT_METADATA_CACHING_ENABLED(OBJECT_METADATA_CACHING_ENABLED_KEY, false),

    /**
     * (string, optional) Controls object metadata caching and eviction. See Guava's CacheBuilderSpec for details.
     * See <a href="https://guava.dev/releases/22.0/api/docs/com/google/common/cache/CacheBuilderSpec.html">https://guava.dev/releases/22.0/api/docs/com/google/common/cache/CacheBuilderSpec.html</a>
     *
     * See {@link BmcConstants#OBJECT_METADATA_CACHING_SPEC_KEY} for config key name. Default is
     * "maximumSize=1024,expireAfterWrite=5m", which caches up to 1024 HEAD object responses for up to 5 minutes.
     *
     * To cache values for an infinite amount of time, do not list any setting for "expireAfterWrite",
     * "expireAfterAccess", or "refreshAfterWrite". For example, "maximumSize=1024" will cache up to 1024 HEAD object
     * responses for an unlimited amount of time, and only re-requesting them if they had to be evicted because of
     * the maximum cache size.
     */
    OBJECT_METADATA_CACHING_SPEC(
            OBJECT_METADATA_CACHING_SPEC_KEY, "maximumSize=1024,expireAfterWrite=5m"),

    /**
     * (boolean, optional) Flag to enable jersey client logging. See
     * {@link BmcConstants#JERSEY_CLIENT_LOGGING_ENABLED_KEY} for config key name. Default is false.
     *
     * Note: If this is enabled, the OCI HDFS Connector will enable the JerseyClientLoggingConfigurator in the underlying
     * java sdk and logs jersey client calls
     */
    JERSEY_CLIENT_LOGGING_ENABLED(JERSEY_CLIENT_LOGGING_ENABLED_KEY, false),

    /**
     * (string, optional) Log Level for jersey client logger. See
     * {@link BmcConstants#JERSEY_CLIENT_LOGGING_LEVEL_KEY} for config key name. Default is Warning.
     *
     * Note: The log level can only be set if jersey client logging flag is enabled
     */
    JERSEY_CLIENT_LOGGING_LEVEL(JERSEY_CLIENT_LOGGING_LEVEL_KEY, "WARNING"),

    /**
     * (string, optional) Verbosity for jersey client logger. See
     * {@link BmcConstants#JERSEY_CLIENT_LOGGING_VERBOSITY_KEY} for config key name. Default is false.
     *
     * Note: The verbosity can only be set if jersey client logging flag is enabled
     */
    JERSEY_CLIENT_LOGGING_VERBOSITY(JERSEY_CLIENT_LOGGING_VERBOSITY_KEY, "PAYLOAD_ANY"),

    /**
     * (boolean, optional) Flag to enable read ahead. This reads bigger blocks from Object Storage, resulting in
     * fewer requests. See {@link BmcConstants#READ_AHEAD_KEY} for config key name. Default is false.
     * Incompatible with IN_MEMORY_READ_.
     */
    READ_AHEAD(READ_AHEAD_KEY, false),

    /**
     * (int, optional) Size in bytes of the blocks to read if READ_AHEAD is enabled.
     * See {@link BmcConstants#READ_AHEAD_BLOCK_SIZE_KEY} for config key name. Default is 6 MiB.
     */
    READ_AHEAD_BLOCK_SIZE(READ_AHEAD_BLOCK_SIZE_KEY, 6 * 1024 * 1024),

    /**
     * (boolean, optional) Flag to enable parquet caching. See
     * {@link BmcConstants#OBJECT_PARQUET_CACHING_ENABLED_KEY} for config key name. Default is false.
     *
     * Requires READ_AHEAD to be enabled.
     *
     * Note: If this is enabled, the OCI HDFS Connector will cache object parquet data, and queries will not necessarily
     * reflect the actual data in Object Storage.
     */
    OBJECT_PARQUET_CACHING_ENABLED(OBJECT_PARQUET_CACHING_ENABLED_KEY, false),

    /**
     * (string, optional) Controls object parquet caching and eviction. See Guava's CacheBuilderSpec for details.
     * See <a href="https://guava.dev/releases/22.0/api/docs/com/google/common/cache/CacheBuilderSpec.html">https://guava.dev/releases/22.0/api/docs/com/google/common/cache/CacheBuilderSpec.html</a>
     *
     * See {@link BmcConstants#OBJECT_PARQUET_CACHING_SPEC_KEY} for config key name. Default is
     * "maximumSize=10240,expireAfterWrite=15m", which caches up to 10240 parquet objects for up to 15 minutes.
     *
     * To cache values for an infinite amount of time, do not list any setting for "expireAfterWrite",
     * "expireAfterAccess", or "refreshAfterWrite". For example, "maximumSize=1024" will cache up to 1024 parquet
     * objects for an unlimited amount of time, and only re-requesting them if they had to be evicted because of
     * the maximum cache size.
     */
    OBJECT_PARQUET_CACHING_SPEC(
            OBJECT_PARQUET_CACHING_SPEC_KEY, "maximumSize=10240,expireAfterWrite=15m"),

    /**
     * (string, optional) Connection Closing Strategy for the Apache Connection. Values supported : IMMEDIATE - for
     * ApacheConnectionClosingStrategy.ImmediateClosingStrategy and
     * GRACEFUL - for ApacheConnectionClosingStrategy.GracefulClosingStrategy
     *
     * If the property is absent then default value of IMMEDIATE will be used.
     *
     * Note: When using ApacheConnectionClosingStrategy.GracefulClosingStrategy, streams returned from response are read
     * till the end of the stream when closing the stream. This can introduce additional time when closing the stream
     * with partial read, depending on how large the remaining stream is. Use
     * ApacheConnectionClosingStrategy.ImmediateClosingStrategy for large files with partial reads instead for faster
     * close.
     * ApacheConnectionClosingStrategy.ImmediateClosingStrategy on the other hand takes longer when using partial
     * read for smaller stream size (less than 1MB). Please consider your use-case and change accordingly.
     * This property is only applicable when using the Apache Connector for sending requests to the service.
     */
    APACHE_CONNECTION_CLOSING_STRATEGY(APACHE_CONNECTION_CLOSING_STRATEGY_KEY, "IMMEDIATE"),

    /**
     * (boolean, optional) Whether payload caching on disk is enabled. Default is false.
     */
    OBJECT_PAYLOAD_CACHING_ENABLED(OBJECT_PAYLOAD_CACHING_ENABLED_KEY, false),

    /**
     * (long, optional) Maximum weight of the cache in bytes. The default size is 4 GiB.
     *
     * Cannot be combined with OBJECT_PAYLOAD_CACHING_MAXIMUM_SIZE.
     */
    OBJECT_PAYLOAD_CACHING_MAXIMUM_WEIGHT_IN_BYTES(
            OBJECT_PAYLOAD_CACHING_MAXIMUM_WEIGHT_IN_BYTES_KEY, 4L * 1024 * 1024 * 1024),

    /**
     * (int, optional) Maximum number of cached items. The default is unset.
     *
     * Cannot be combined with OBJECT_PAYLOAD_CACHING_MAXIMUM_WEIGHT_IN_BYTES.
     */
    OBJECT_PAYLOAD_CACHING_MAXIMUM_SIZE(OBJECT_PAYLOAD_CACHING_MAXIMUM_SIZE_KEY, null),

    /**
     * (int, optional) Initial capacity of cached items. The default is 1024.
     */
    OBJECT_PAYLOAD_CACHING_INITIAL_CAPACITY(OBJECT_PAYLOAD_CACHING_INITIAL_CAPACITY_KEY, 1024),

    /**
     * (boolean, optional) Whether to record cache statistics. The default is false.
     */
    OBJECT_PAYLOAD_CACHING_RECORD_STATS_ENABLED(
            OBJECT_PAYLOAD_CACHING_RECORD_STATS_ENABLED_KEY, false),

    /**
     * (long, optional) The time interval (in seconds) between successive logging of cache statistics. The default is 60 seconds.
     */
    OBJECT_PAYLOAD_CACHING_RECORD_STATS_TIME_INTERVAL_IN_SECONDS(
            OBJECT_PAYLOAD_CACHING_RECORD_STATS_TIME_INTERVAL_IN_SECONDS_KEY, 60L),

    /**
     * (int, optional) Whether cached items should be expired if a certain number of seconds has passed since the
     * last access, regardless of whether it was a read or a write. The default is unset, which means this expiration
     * strategy is not used.
     *
     * Cannot be combined with OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_WRITE_SECONDS.
     */
    OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_ACCESS_SECONDS(
            OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_ACCESS_SECONDS_KEY, null),

    /**
     * (int, optional) Whether cached items should be expired if a certain number of seconds has passed since the
     * last write access. The default is 600 (10 minutes).
     *
     * Cannot be combined with OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_WRITE_SECONDS.
     */
    OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_WRITE_SECONDS(
            OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_WRITE_SECONDS_KEY,
            (int) TimeUnit.MINUTES.toSeconds(10)),

    /**
     * (string, optional) The consistency policy to use for the object payload cache. The default is
     * "com.oracle.bmc.hdfs.caching.StrongConsistencyPolicy", which checks if the object was updated on the server.
     * If you know your data does not change, you can set it to "com.oracle.bmc.hdfs.caching.NoOpConsistencyPolicy".
     */
    OBJECT_PAYLOAD_CACHING_CONSISTENCY_POLICY_CLASS(
            OBJECT_PAYLOAD_CACHING_CONSISTENCY_POLICY_CLASS_KEY,
            StrongConsistencyPolicy.class.getName()),

    /**
     * (string, optional) The directory for the object payload cache. The default is the value of the "java.io.tmpdir"
     * property.
     */
    OBJECT_PAYLOAD_CACHING_DIRECTORY(OBJECT_PAYLOAD_CACHING_DIRECTORY_KEY, null),

    /**
     * (boolean, optional) Flag to enable Jersey default HttpUrlConnector for sending requests using jersey client. See
     * {@link BmcConstants#JERSEY_CLIENT_DEFAULT_CONNECTOR_ENABLED_KEY} for config key name. Default is false.
     *
     * Note: If the SDK default ApacheConnector is to be utilised for sending requests, please don't change this value
     */
    JERSEY_CLIENT_DEFAULT_CONNECTOR_ENABLED(JERSEY_CLIENT_DEFAULT_CONNECTOR_ENABLED_KEY, false),

    /**
     * (int, optional) The maximum number of connections that will be pooled in the connection pool used by the Apache
     * Client to create connections with the service. Note, any value smaller than 0 is interpreted as using the default Java SDK value.
     * See {@link com.oracle.bmc.http.ApacheConnectorProperties}.
     */
    APACHE_MAX_CONNECTION_POOL_SIZE(APACHE_MAX_CONNECTION_POOL_SIZE_KEY, 50),

    /**
     * (int, optional) The number of threads to use for parallel rename operations when renaming the directory.
     * Note, any value smaller than 0 is interpreted as using the default value. Default is 1.
     * See {@link com.oracle.bmc.http.ApacheConnectorProperties}.
     */
    RENAME_DIRECTORY_NUM_THREADS(RENAME_DIRECTORY_NUM_THREADS_KEY, 1),

    /**
     * (boolean, optional) Flag to enable pseudo-streaming to OCI via Multipart Uploads backed by a circular buffer.
     * See {@link BmcConstants#MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED_KEY} for config key name. Default is false.
     *
     * Cannot be enabled at the same time as {@link BmcProperties#IN_MEMORY_WRITE_BUFFER}.
     *
     * If enabled, requires {@link BmcProperties#MULTIPART_NUM_UPLOAD_THREADS} to be set.
     */
    MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED(MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED_KEY, false),

    /**
     * (int, optional) The amount of time in seconds to block waiting for a slot in the multipart upload executor.
     * See {@link BmcConstants#MULTIPART_IN_MEMORY_WRITE_TASK_TIMEOUT_SECONDS_KEY} for config key name. Default is 900.
     */
    MULTIPART_IN_MEMORY_WRITE_TASK_TIMEOUT_SECONDS(
            MULTIPART_IN_MEMORY_WRITE_TASK_TIMEOUT_SECONDS_KEY, 900),

    /**
     * (boolean, optional) Flag to enable overwrites while using Multipart Uploads.
     * See {@link BmcConstants#MULTIPART_ALLOW_OVERWRITE_KEY} for config key name. Default is false.
     */
    MULTIPART_ALLOW_OVERWRITE(MULTIPART_ALLOW_OVERWRITE_KEY, false),

    /**
     * (boolean, optional) Flag to enable/disable auto-close of object streams on full read. The default is true.
     */
    OBJECT_AUTO_CLOSE_INPUT_STREAM(OBJECT_AUTO_CLOSE_INPUT_STREAM_KEY, true),

    /**
     * (boolean, optional) Flag to enable/disable the use of realm-specific endpoint templates, if defined. Default is false. See {@link BmcConstants#REALM_SPECIFIC_ENDPOINT_TEMPLATES_ENABLED_KEY} for config key name
     * If enabled, requires {@link BmcProperties#REGION_CODE_OR_ID} to be set.
     * Note, this field will be overridden by "fs.oci.client.hostname property" if set.
     */
    REALM_SPECIFIC_ENDPOINT_TEMPLATES_ENABLED(REALM_SPECIFIC_ENDPOINT_TEMPLATES_ENABLED_KEY, false),


    /**
     * (string, optional) The custom stream class for reading.
     * Fully qualified class name of a custom implementation of {@link com.oracle.bmc.hdfs.store.AbstractBmcCustomFSInputStream}. See
     * {@link BmcConstants#READ_STREAM_CLASS_KEY} for config key name.
     */
    READ_STREAM_CLASS(READ_STREAM_CLASS_KEY, null),

    /**
     * (string, optional) The custom stream class for writing.
     * Fully qualified class name of a custom implementation of {@link com.oracle.bmc.hdfs.store.AbstractBmcCustomOutputStream}. See
     * {@link BmcConstants#WRITE_STREAM_CLASS_KEY} for config key name.
     */
    WRITE_STREAM_CLASS(WRITE_STREAM_CLASS_KEY, null),
    // BmcFilesystem instance caching
    // These cannot be overridden per namespace and bucket.

    /**
     * (boolean, optional) Whether {@link BmcFilesystem} caching is enabled. Default is false.
     *
     * This setting cannot be overridden per namespace and bucket.
     */
    FILESYSTEM_CACHING_ENABLED(FILESYSTEM_CACHING_ENABLED_KEY, false),

    /**
     * (int, optional) Maximum number of cached {@link BmcFilesystem} items. The default is 1024.
     *
     * This setting cannot be overridden per namespace and bucket.
     */
    FILESYSTEM_CACHING_MAXIMUM_SIZE(FILESYSTEM_CACHING_MAXIMUM_SIZE_KEY, 1024),

    /**
     * (int, optional) Initial capacity of cached {@link BmcFilesystem} items. The default is 1024.
     *
     * This setting cannot be overridden per namespace and bucket.
     */
    FILESYSTEM_CACHING_INITIAL_CAPACITY(FILESYSTEM_CACHING_INITIAL_CAPACITY_KEY, 1024),

    /**
     * (int, optional) Whether cached {@link BmcFilesystem} items should be expired if a certain number of seconds has
     * passed since the last access, regardless of whether it was a read or a write. The default is unset, which means
     * this expiration strategy is not used.
     *
     * Cannot be combined with FILESYSTEM_CACHING_EXPIRE_AFTER_WRITE_SECONDS.
     *
     * This setting cannot be overridden per namespace and bucket.
     */
    FILESYSTEM_CACHING_EXPIRE_AFTER_ACCESS_SECONDS(
            FILESYSTEM_CACHING_EXPIRE_AFTER_ACCESS_SECONDS_KEY, null),

    /**
     * (int, optional) Whether cached {@link BmcFilesystem} items should be expired if a certain number of seconds has
     * passed since the last write access. The default is 30 minutes.
     *
     * Cannot be combined with FILESYSTEM_CACHING_EXPIRE_AFTER_ACCESS_SECONDS.
     *
     * This setting cannot be overridden per namespace and bucket.
     */
    FILESYSTEM_CACHING_EXPIRE_AFTER_WRITE_SECONDS(
            FILESYSTEM_CACHING_EXPIRE_AFTER_WRITE_SECONDS_KEY,
            (int) TimeUnit.MINUTES.toSeconds(30)),

    /**
     * (String, optional) File path that has the delegation token. Default value is null
     */
    OCI_DELEGATION_TOKEN_FILEPATH(OCI_DELEGATION_TOKEN_FILEPATH_KEY, null),
    ;

    @Getter private final String propertyName;
    @Getter private final Object defaultValue;
}
