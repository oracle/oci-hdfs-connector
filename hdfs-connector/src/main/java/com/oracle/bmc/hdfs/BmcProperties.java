/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs;

import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.hdfs.auth.spnego.UPSTAuthenticationCustomAuthenticator;
import com.oracle.bmc.hdfs.caching.StrongConsistencyPolicy;
import com.oracle.bmc.hdfs.monitoring.OCIMonitorConsumerPlugin;
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
     * (boolean, optional) Flag to enable the SDK level circuit breaker.
     * See {@link BmcConstants#OBJECT_STORAGE_CLIENT_CIRCUIT_BREAKER_ENABLED_KEY} for config key name. Default is true.
     */
    OBJECT_STORAGE_CLIENT_CIRCUIT_BREAKER_ENABLED(OBJECT_STORAGE_CLIENT_CIRCUIT_BREAKER_ENABLED_KEY, true),

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
     * (string, optional) Service Principal required to use for generating SPNEGO token.
     * Note: This configuration is only relevant when using {@link UPSTAuthenticationCustomAuthenticator}..
     * See {@link BmcConstants#TOKEN_EXCHANGE_SERVICE_PRINCIPAL_KEY} for config key name.
     */
    TOKEN_EXCHANGE_SERVICE_PRINCIPAL(TOKEN_EXCHANGE_SERVICE_PRINCIPAL_KEY, null),
    /**
     * (string, optional) User Principal required to use for generating SPNEGO token.
     * Note: This configuration is only relevant when using {@link UPSTAuthenticationCustomAuthenticator}.
     * See {@link BmcConstants#TOKEN_EXCHANGE_SERVICE_PRINCIPAL_KEY} for config key name.
     */
    TOKEN_EXCHANGE_USER_PRINCIPAL(TOKEN_EXCHANGE_USER_PRINCIPAL_KEY, null),
    /**
     * (string, optional) Issuer required to use for generating SPNEGO token.
     * Note: This configuration is only relevant when using {@link UPSTAuthenticationCustomAuthenticator}.
     * See {@link BmcConstants#TOKEN_EXCHANGE_SERVICE_ISSUER_KEY} for config key name.
     */
    TOKEN_EXCHANGE_SERVICE_ISSUER(TOKEN_EXCHANGE_SERVICE_ISSUER_KEY, null),
    /**
     * (string, optional) The domain application client ID used for IAM token exchange.
     * This is used to identify the domain application for which IAM token is being requested.
     * Note: This configuration is only relevant when using {@link UPSTAuthenticationCustomAuthenticator}.
     * See {@link BmcConstants#IAM_DOMAIN_APP_CLIENT_ID_KEY} for config key name.
     */
    IAM_DOMAIN_APP_CLIENT_ID(IAM_DOMAIN_APP_CLIENT_ID_KEY, null),

    /**
     * (string, optional) The domain application client secret used for IAM token exchange.
     * This secret acts as a password for the corresponding client ID, ensuring that only
     * authorized applications can request IAM tokens.
     * Note: This configuration is only relevant when using {@link UPSTAuthenticationCustomAuthenticator}.
     * See {@link BmcConstants#IAM_DOMAIN_APP_CLIENT_SECRET_KEY} for config key name.
     */
    IAM_DOMAIN_APP_CLIENT_SECRET(IAM_DOMAIN_APP_CLIENT_SECRET_KEY, null),
    /**
     * (string, required) The endpoint URL for the IAM token exchange API. This is where
     * the SPNEGO token will be exchanged for an IAM token.
     * Note: This configuration is only relevant when using {@link UPSTAuthenticationCustomAuthenticator}.
     * See {@link BmcConstants#IAM_TOKEN_EXCHANGE_ENDPOINT_URL_KEY} for config key name.
     */
    IAM_TOKEN_EXCHANGE_ENDPOINT_URL(IAM_TOKEN_EXCHANGE_ENDPOINT_URL_KEY, null),
    /**
     * (string, required) The file path to the keytab used for the token exchange process.
     * This keytab is essential when opting for the Kerberos authentication mechanism for token generation.
     * Note: This configuration is only relevant when using {@link UPSTAuthenticationCustomAuthenticator}.
     * See {@link BmcConstants#IAM_TOKEN_EXCHANGE_KEYTAB_PATH_KEY} for config key name.
     */
    IAM_TOKEN_EXCHANGE_KEYTAB_PATH(IAM_TOKEN_EXCHANGE_KEYTAB_PATH_KEY, null),
    /**
     * (boolean, optional) Flag indicating the mechanism for SPNEGO token generation.
     *
     * If set to true (internal):
     * The connector will handle the `kinit` process programmatically. Users must provide the
     * path to the keytab file via the "IAM_TOKEN_EXCHANGE_KEYTAB_PATH" configuration.
     * The connector will then use this keytab to programmatically obtain and cache the
     * necessary Kerberos ticket for SPNEGO token generation.
     *
     * If set to false (external):
     * The connector expects that users have already invoked `kinit` or an equivalent external process
     * using an external Kerberos client to obtain and cache the necessary Kerberos ticket before using the connector.
     * Failing to do so may result in authentication failures.
     * Note: This configuration is only relevant when using {@link UPSTAuthenticationCustomAuthenticator}.
     * See {@link BmcConstants#ENABLE_INTERNAL_KINIT_FOR_TOKEN_EXCHANGE_KEY} for config key name.
     */
    ENABLE_INTERNAL_KINIT_FOR_TOKEN_EXCHANGE(ENABLE_INTERNAL_KINIT_FOR_TOKEN_EXCHANGE_KEY, false),
    /**
     * (string, optional) The region code or region identifier used to establish Object Storage endpoint name.
     * Note, if "fs.oci.client.hostname property" is set, then this field is not required.
     * See {@link BmcConstants#REGION_CODE_OR_ID_KEY} for config key name.
     */
    REGION_CODE_OR_ID(REGION_CODE_OR_ID_KEY, null),
    /**
      * (boolean, optional) Enable multi-region support. Users have the ability to connect to multiple regions by
     * specifying region in the connection URI. URI format (oci://bucket@namespace.region/path).
      * Other properties related to endpoint hostnames, such as "fs.oci.client.hostname",
     * "fs.oci.client.regionCodeOrId" and "fs.oci.realmspecific.endpoint.template.enabled" will not be ignored.
      */
    MULTIREGION_ENABLED(MULTIREGION_ENABLED_KEY, false),
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
     * (int, optional) The maximum number of retries if data reading fails. See
     * {@link BmcConstants#READ_MAX_RETRIES_KEY}
     * default 3, retry once if the initial read attempt failed.
     */
    READ_MAX_RETRIES(READ_MAX_RETRIES_KEY, 3),

    /**
     * (int, optional) The maximum number of retries if data writing fails. See
     * {@link BmcConstants#WRITE_MAX_RETRIES_KEY}
     * default 3, retry once if the initial write attempt failed.
     */
    WRITE_MAX_RETRIES(WRITE_MAX_RETRIES_KEY, 3),

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
     * (int, optional) Number of blocks to read if READ_AHEAD is enabled.
     * See {@link BmcConstants#READ_AHEAD_BLOCK_COUNT_KEY} for config key name. Default is 1.
     */
    READ_AHEAD_BLOCK_COUNT(READ_AHEAD_BLOCK_COUNT_KEY, 1),

    /**
     * (int, optional) The number of threads to use for parallel GET operations when reading ahead.
     * See {@link BmcConstants#NUM_READ_AHEAD_THREADS_KEY} for config key name. Note, any value
     * smaller than 0 is interpreted as using the default value. Default is 16.
     */
    NUM_READ_AHEAD_THREADS(NUM_READ_AHEAD_THREADS_KEY, 16),


    /**
     * (long,optional) Timeout for the I/O threads created to make operations to the Object Storage service.
     * See {@link BmcConstants#DATASTORE_IO_THREAD_TIMEOUT_IN_SECONDS_KEY} for config key name. Default is 10 minutes.
     * Smaller than or equal to 0 is interpreted as using the default value.
     */
    BMC_DATASTORE_IO_THREAD_TIMEOUT_IN_SECONDS(DATASTORE_IO_THREAD_TIMEOUT_IN_SECONDS_KEY,600L),

    /**
     * (boolean, optional) When set to true, enforces strict directory marker rules for the file system represented by the Object Storage bucket.
     * Each directory must be represented by a zero-byte object with the (directory name + '/') as the object name.
     * If set to false, it allows the possibility of directories without zero-byte objects representing them.
     * For example, if file "/a/b" exists but "/a/" does not exist, it still considers the directory "/a/" as existing due to the presence of the file "/a/b".
     * If fs.oci.require.directory.marker is set to true, the status of file "/a/b" exists but "/a/" does not exist is considered invalid.
     */
    REQUIRE_DIRECTORY_MARKER(REQUIRE_DIRECTORY_MARKER_KEY, false),

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
     * Deprecated use {@link BmcConstants#OBJECT_ALLOW_OVERWRITE_KEY} instead.
     */
    MULTIPART_ALLOW_OVERWRITE(MULTIPART_ALLOW_OVERWRITE_KEY, false),

    /**
     * (boolean, optional) Flag to enable object or part overwrites during retry or concurrent write.
     * See {@link BmcConstants#OBJECT_ALLOW_OVERWRITE_KEY} for config key name. Default is true.
     */
    OBJECT_ALLOW_OVERWRITE(OBJECT_ALLOW_OVERWRITE_KEY, true),
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

    /**
     * (int, optional) Whenever listFiles with recursive being true is invoked on a dir, a listing API is called
     * on OSS that gets all the files in a flat manner. This property determines what is the max number of files
     * to be listed in one call to OSS Service (essentially the page size of the OSS listing API).
     * See {@link BmcConstants#RECURSIVE_DIR_LISTING_FETCH_SIZE_KEY} for config key name.
     */
    RECURSIVE_DIR_LISTING_FETCH_SIZE(RECURSIVE_DIR_LISTING_FETCH_SIZE_KEY, 1000),

    /**
     * (String, optional) This defines what plugins are available to consume the metrics collected by the OCI
     * monitoring framework. This has to be the fully qualified classname of the class extending the
     * {@link OCIMonitorConsumerPlugin} class. Comma separated values are accepted.
     * for multiple plugins.
     * See {@link BmcConstants#OCI_MON_TELEMETRY_INGESTION_ENDPOINT_KEY} for config key name.
     */
    OCI_MONITORING_CONSUMER_PLUGINS(OCI_MONITORING_CONSUMER_PLUGINS_KEY,
            "com.oracle.bmc.hdfs.monitoring.OCIMonitorPlugin"),

    /**
     * (String, optional) The endpoint for telemetry ingestion. This is required if OCI monitoring is enabled.
     * A typical endpoint is like the following: https://telemetry-ingestion.us-ashburn-1.oraclecloud.com
     * See {@link BmcConstants#OCI_MON_TELEMETRY_INGESTION_ENDPOINT_KEY} for config key name.
     */
    OCI_MON_TELEMETRY_INGESTION_ENDPOINT(OCI_MON_TELEMETRY_INGESTION_ENDPOINT_KEY, null),

    /**
     * (String, optional) The compartment OCID to which the metrics will be emitted. This compartment should
     * have the right Identity policies setup for receiving monitoring information. This field is mandatory only if
     * the OCI monitoring is enabled.
     * See {@link BmcConstants#OCI_MON_COMPARTMENT_OCID_KEY} for config key name.
     */
    OCI_MON_COMPARTMENT_OCID(OCI_MON_COMPARTMENT_OCID_KEY, null),

    /**
     * (String, optional) This is the unique identifier used to group analytics values. For example it could be the
     * ID of the HDFS cluster. All the metrics belonging to this ID are grouped together on OCI monitoring no matter
     * where they are emitted from. This field is mandatory only if OCI monitoring is enabled.
     * See {@link BmcConstants#OCI_MON_GROUPING_CLUSTER_ID_KEY} for config key name.
     */
    OCI_MON_GROUPING_CLUSTER_ID(OCI_MON_GROUPING_CLUSTER_ID_KEY, null),

    /**
     * (String, optional) This is the resource group name on the OCI monitoring.
     * See {@link BmcConstants#OCI_MON_RG_NAME_KEY} for config key name.
     */
    OCI_MON_RG_NAME(OCI_MON_RG_NAME_KEY, "apicallstest"),

    /**
     * (String, optional) This is the namespace name on the OCI monitoring.
     * See {@link BmcConstants#OCI_MON_NS_NAME_KEY} for config key name.
     */
    OCI_MON_NS_NAME(OCI_MON_NS_NAME_KEY, "ocihdfsconnector"),

    /**
     * (boolean, optional) This field determines if the stats are emitted with bucket name as a dimension or not.
     * See {@link BmcConstants#OCI_MON_BUCKET_LEVEL_ENABLED_KEY} for config key name.
     */
    OCI_MON_BUCKET_LEVEL_ENABLED(OCI_MON_BUCKET_LEVEL_ENABLED_KEY, true),

    /**
     * (Integer, optional) The sleep time for the monitor thread in between runs.
     * See {@link BmcConstants#OCI_MON_EMIT_THREAD_POLL_INTERVAL_SECONDS_KEY} for config key name.
     */
    OCI_MON_EMIT_THREAD_POLL_INTERVAL_SECONDS(OCI_MON_EMIT_THREAD_POLL_INTERVAL_SECONDS_KEY, 2),

    /**
     * (Integer, optional) The maximum backlog that will be allowed in the list before no-admit.
     * See {@link BmcConstants#OCI_MON_MAX_BACKLOG_BEFORE_DROP_KEY} for config key name.
     */
    OCI_MON_MAX_BACKLOG_BEFORE_DROP(OCI_MON_MAX_BACKLOG_BEFORE_DROP_KEY, 100000)
    ;

    @Getter private final String propertyName;
    @Getter private final Object defaultValue;
}
