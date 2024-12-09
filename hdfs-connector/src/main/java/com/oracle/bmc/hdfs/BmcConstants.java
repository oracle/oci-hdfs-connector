/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs;

import com.google.common.collect.ImmutableMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Map;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class BmcConstants {
    public static final String OCI_SCHEME = "oci";

    // -1 means no default port
    static final int DEFAULT_PORT = -1;

    public static final String BLOCK_SIZE_IN_MB_KEY = "fs.oci.blocksize.mb";

    public static final String IN_MEMORY_WRITE_BUFFER_KEY = "fs.oci.io.write.inmemory";

    public static final String IN_MEMORY_READ_BUFFER_KEY = "fs.oci.io.read.inmemory";

    public static final String MULTIPART_IN_MEMORY_WRITE_BUFFER_ENABLED_KEY =
            "fs.oci.io.write.multipart.inmemory";

    public static final String MULTIPART_IN_MEMORY_WRITE_TASK_TIMEOUT_SECONDS_KEY =
            "fs.oci.io.write.multipart.inmemory.tasktimeout.seconds";

    public static final String MULTIPART_ALLOW_OVERWRITE_KEY =
            "fs.oci.io.write.multipart.overwrite";

    public static final String READ_AHEAD_KEY = "fs.oci.io.read.ahead";

    public static final String READ_AHEAD_BLOCK_SIZE_KEY = "fs.oci.io.read.ahead.blocksize";

    public static final String READ_STREAM_CLASS_KEY = "fs.oci.io.read.custom.stream";

    public static final String READ_AHEAD_BLOCK_COUNT_KEY = "fs.oci.io.read.ahead.blockcount";

    public static final String NUM_READ_AHEAD_THREADS_KEY = "fs.oci.io.read.ahead.numthreads";

    public static final String DATASTORE_IO_THREAD_TIMEOUT_IN_SECONDS_KEY = "fs.oci.io.threads.timeout.seconds";

    public static final String WRITE_STREAM_CLASS_KEY = "fs.oci.io.write.custom.stream";

    public static final String OBJECT_STORE_CLIENT_CLASS_KEY = "fs.oci.client.custom.client";

    public static final String OBJECT_STORE_AUTHENTICATOR_CLASS_KEY =
            "fs.oci.client.custom.authenticator";

    public static final String HOST_NAME_KEY = "fs.oci.client.hostname";

    public static final String TENANT_ID_KEY = "fs.oci.client.auth.tenantId";

    public static final String USER_ID_KEY = "fs.oci.client.auth.userId";

    public static final String FINGERPRINT_KEY = "fs.oci.client.auth.fingerprint";

    public static final String PEM_FILE_PATH_KEY = "fs.oci.client.auth.pemfilepath";

    public static final String PASS_PHRASE_KEY = "fs.oci.client.auth.passphrase";

    public static final String TOKEN_EXCHANGE_SERVICE_PRINCIPAL_KEY = "fs.oci.client.upst.tokenExchangeServicePrincipal";

    public static final String TOKEN_EXCHANGE_USER_PRINCIPAL_KEY = "fs.oci.client.upst.userPrincipal";

    public static final String TOKEN_EXCHANGE_SERVICE_ISSUER_KEY = "fs.oci.client.upst.issuer";

    public static final String IAM_DOMAIN_APP_CLIENT_ID_KEY = "fs.oci.client.upst.clientId";

    public static final String IAM_DOMAIN_APP_CLIENT_SECRET_KEY = "fs.oci.client.upst.clientSecret";

    public static final String IAM_TOKEN_EXCHANGE_ENDPOINT_URL_KEY = "fs.oci.client.upst.domainUrl";

    public static final String IAM_TOKEN_EXCHANGE_KEYTAB_PATH_KEY = "fs.oci.client.keytab.path";

    public static final String ENABLE_INTERNAL_KINIT_FOR_TOKEN_EXCHANGE_KEY = "fs.oci.client.kinit.internal.mode";

    public static final String REGION_CODE_OR_ID_KEY = "fs.oci.client.regionCodeOrId";

    public static final String CONNECTION_TIMEOUT_MILLIS_KEY =
            "fs.oci.client.http.connectiontimeout";

    public static final String MULTIREGION_ENABLED_KEY = "fs.oci.client.multiregion.enabled";

    public static final String READ_TIMEOUT_MILLIS_KEY = "fs.oci.client.http.readtimeout";

    public static final String  READ_MAX_RETRIES_KEY = "fs.oci.client.http.readmaxretries";

    public static final String MULTIPART_ALLOWED_KEY = "fs.oci.client.multipart.allowed";

    public static final String MULTIPART_NUM_UPLOAD_THREADS_KEY =
            "fs.oci.client.multipart.numthreads";

    public static final String MD5_NUM_THREADS_KEY =
            "fs.oci.client.md5.numthreads";
    public static final String MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB_KEY =
            "fs.oci.client.multipart.minobjectsize.mb";

    /** @deprecated use {@link #MULTIPART_PART_SIZE_IN_MB_KEY} instead */
    @java.lang.Deprecated
    public static final String MULTIPART_MIN_PART_SIZE_IN_MB_KEY =
            "fs.oci.client.multipart.minpartsize.mb";

    public static final String MULTIPART_PART_SIZE_IN_MB_KEY =
            "fs.oci.client.multipart.partsize.mb";

    public static final String HTTP_PROXY_URI_KEY = "fs.oci.client.proxy.uri";

    public static final String HTTP_PROXY_USERNAME_KEY = "fs.oci.client.proxy.username";

    public static final String HTTP_PROXY_PASSWORD_KEY = "fs.oci.client.proxy.password";

    public static final String RETRY_TIMEOUT_IN_SECONDS_KEY = "fs.oci.client.retry.timeout.seconds";

    public static final String RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS_KEY =
            "fs.oci.client.retry.reset.threshold.seconds";

    public static final String OBJECT_METADATA_CACHING_ENABLED_KEY =
            "fs.oci.caching.object.metadata.enabled";

    public static final String OBJECT_METADATA_CACHING_SPEC_KEY =
            "fs.oci.caching.object.metadata.spec";

    public static final String JERSEY_CLIENT_LOGGING_ENABLED_KEY =
            "fs.oci.client.jersey.logging.enabled";

    public static final String JERSEY_CLIENT_LOGGING_LEVEL_KEY =
            "fs.oci.client.jersey.logging.level";

    public static final String JERSEY_CLIENT_LOGGING_VERBOSITY_KEY =
            "fs.oci.client.jersey.logging.verbosity";

    public static final String OBJECT_PARQUET_CACHING_ENABLED_KEY =
            "fs.oci.caching.object.parquet.enabled";

    public static final String OBJECT_PARQUET_CACHING_SPEC_KEY =
            "fs.oci.caching.object.parquet.spec";

    public static final String RENAME_DIRECTORY_NUM_THREADS_KEY =
            "fs.oci.rename.operation.numthreads";

    public static final String APACHE_CONNECTION_CLOSING_STRATEGY_KEY =
            "fs.oci.client.apache.connection.closing.strategy";

    public static final String OBJECT_PAYLOAD_CACHING_ENABLED_KEY =
            "fs.oci.caching.object.payload.enabled";

    public static final String REQUIRE_DIRECTORY_MARKER_KEY =
            "fs.oci.require.directory.marker";

    public static final String OBJECT_PAYLOAD_CACHING_MAXIMUM_WEIGHT_IN_BYTES_KEY =
            "fs.oci.caching.object.payload.maxweight.bytes";

    public static final String OBJECT_PAYLOAD_CACHING_MAXIMUM_SIZE_KEY =
            "fs.oci.caching.object.payload.maxsize.count";

    public static final String OBJECT_PAYLOAD_CACHING_INITIAL_CAPACITY_KEY =
            "fs.oci.caching.object.payload.initialcapacity.count";

    public static final String OBJECT_PAYLOAD_CACHING_RECORD_STATS_ENABLED_KEY =
            "fs.oci.caching.object.payload.recordstats.enabled";

    public static final String OBJECT_PAYLOAD_CACHING_RECORD_STATS_TIME_INTERVAL_IN_SECONDS_KEY =
            "fs.oci.caching.object.payload.recordstats.timeinterval.seconds";

    public static final String OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_ACCESS_SECONDS_KEY =
            "fs.oci.caching.object.payload.expireafteraccess.seconds";

    public static final String OBJECT_PAYLOAD_CACHING_EXPIRE_AFTER_WRITE_SECONDS_KEY =
            "fs.oci.caching.object.payload.expireafterwrite.seconds";

    public static final String OBJECT_PAYLOAD_CACHING_CONSISTENCY_POLICY_CLASS_KEY =
            "fs.oci.caching.object.payload.consistencypolicy.class";

    public static final String OBJECT_PAYLOAD_CACHING_DIRECTORY_KEY =
            "fs.oci.caching.object.payload.directory";

    public static final String JERSEY_CLIENT_DEFAULT_CONNECTOR_ENABLED_KEY =
            "fs.oci.client.jersey.default.connector.enabled";

    public static final String APACHE_MAX_CONNECTION_POOL_SIZE_KEY =
            "fs.oci.client.apache.max.connection.pool.size";

    public static final String OBJECT_AUTO_CLOSE_INPUT_STREAM_KEY =
            "fs.oci.object.autoclose.inputstream";

    public static final String REALM_SPECIFIC_ENDPOINT_TEMPLATES_ENABLED_KEY = "fs.oci.realmspecific.endpoint.template.enabled";

    // BmcFilesystem instance caching

    public static final String FILESYSTEM_CACHING_ENABLED_KEY = "fs.oci.caching.filesystem.enabled";

    public static final String FILESYSTEM_CACHING_MAXIMUM_SIZE_KEY =
            "fs.oci.caching.filesystem.maxsize.count";

    public static final String FILESYSTEM_CACHING_INITIAL_CAPACITY_KEY =
            "fs.oci.caching.filesystem.initialcapacity.count";

    public static final String FILESYSTEM_CACHING_EXPIRE_AFTER_ACCESS_SECONDS_KEY =
            "fs.oci.caching.filesystem.expireafteraccess.seconds";

    public static final String FILESYSTEM_CACHING_EXPIRE_AFTER_WRITE_SECONDS_KEY =
            "fs.oci.caching.filesystem.expireafterwrite.seconds";

    public static final String OCI_DELEGATION_TOKEN_FILEPATH_KEY =
            "fs.oci.delegation.token.filepath";

    public static final String RECURSIVE_DIR_LISTING_FETCH_SIZE_KEY = "fs.oci.dir.recursive.list.fetch.size";

    /**
     * Following are the properties related to OCI monitoring for the connector
     */
    public static final String OCI_MONITORING_CONSUMER_PLUGINS_KEY = "fs.oci.mon.consumer.plugins";
    public static final String OCI_MON_TELEMETRY_INGESTION_ENDPOINT_KEY = "fs.oci.mon.telemetry.ingestion.endpoint";
    public static final String OCI_MON_COMPARTMENT_OCID_KEY = "fs.oci.mon.compartment.ocid";
    public static final String OCI_MON_GROUPING_CLUSTER_ID_KEY = "fs.oci.mon.grouping.cluster.id";
    public static final String OCI_MON_RG_NAME_KEY = "fs.oci.mon.rg.name";
    public static final String OCI_MON_NS_NAME_KEY = "fs.oci.mon.ns.name";
    public static final String OCI_MON_BUCKET_LEVEL_ENABLED_KEY = "fs.oci.mon.bucket.level.enabled";
    public static final String OCI_MON_EMIT_THREAD_POLL_INTERVAL_SECONDS_KEY
            = "fs.oci.emit.thread.poll.interval.seconds";
    public static final String OCI_MON_MAX_BACKLOG_BEFORE_DROP_KEY = "fs.oci.mon.max.backlog.before.drop";

    // sdk circuit breaker conf
    public static final String OBJECT_STORAGE_CLIENT_CIRCUIT_BREAKER_ENABLED_KEY = "fs.oci.client.circuitbreaker.enabled";

    /**
     * This class contains constants with deprecated values. The HDFS connector will first try the current values
     * in {@link com.oracle.bmc.hdfs.BmcConstants}.
     */
    public enum Deprecated {
        ; // no-value enum to prevent instantiation
        public static final String BMC_SCHEME = "oraclebmc";

        // you only have to add mappings from new key to old key if the mapping doesn't follow the
        // "fs.oci.<REST>" -> "fs.oraclebmc.<REST>" rule
        private static final Map<String, String> NEW_TO_OLD_KEYS =
                ImmutableMap.<String, String>builder()
                        .put(BLOCK_SIZE_IN_MB_KEY, "fs.oraclebmc.blocksize.mb")
                        .put(IN_MEMORY_WRITE_BUFFER_KEY, "fs.oraclebmc.io.write.inmemory")
                        .put(IN_MEMORY_READ_BUFFER_KEY, "fs.oraclebmc.io.read.inmemory")
                        .put(OBJECT_STORE_CLIENT_CLASS_KEY, "fs.oraclebmc.client.custom.client")
                        .put(
                                OBJECT_STORE_AUTHENTICATOR_CLASS_KEY,
                                "fs.oraclebmc.client.custom.authenticator")
                        .put(HOST_NAME_KEY, "fs.oraclebmc.client.hostname")
                        .put(TENANT_ID_KEY, "fs.oraclebmc.client.auth.tenantId")
                        .put(USER_ID_KEY, "fs.oraclebmc.client.auth.userId")
                        .put(FINGERPRINT_KEY, "fs.oraclebmc.client.auth.fingerprint")
                        .put(PEM_FILE_PATH_KEY, "fs.oraclebmc.client.auth.pemfilepath")
                        .put(PASS_PHRASE_KEY, "fs.oraclebmc.client.auth.passphrase")
                        .put(REGION_CODE_OR_ID_KEY, "fs.oraclebmc.client.regionCodeOrId")
                        .put(MULTIREGION_ENABLED_KEY, "fs.oraclebmc.client.multiregion.enabled")
                        .put(
                                CONNECTION_TIMEOUT_MILLIS_KEY,
                                "fs.oraclebmc.client.http.connectiontimeout")
                        .put(READ_TIMEOUT_MILLIS_KEY, "fs.oraclebmc.client.http.readtimeout")
                        .put(READ_MAX_RETRIES_KEY, "fs.oraclebmc.client.http.readmaxretries")
                        .put(MULTIPART_ALLOWED_KEY, "fs.oraclebmc.client.multipart.allowed")
                        .put(
                                MULTIPART_NUM_UPLOAD_THREADS_KEY,
                                "fs.oraclebmc.client.multipart.numthreads")
                        .put(
                                MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB_KEY,
                                "fs.oraclebmc.client.multipart.minobjectsize.mb")
                        .put(
                                MULTIPART_PART_SIZE_IN_MB_KEY,
                                "fs.oraclebmc.client.multipart.partsize.mb")
                        /* Deprecated */
                        .put(
                                MULTIPART_MIN_PART_SIZE_IN_MB_KEY,
                                "fs.oraclebmc.client.multipart.minpartsize.mb")
                        .put(HTTP_PROXY_URI_KEY, "fs.oraclebmc.client.proxy.uri")
                        .put(HTTP_PROXY_USERNAME_KEY, "fs.oraclebmc.client.proxy.username")
                        .put(HTTP_PROXY_PASSWORD_KEY, "fs.oraclebmc.client.proxy.password")
                        .put(
                                RETRY_TIMEOUT_IN_SECONDS_KEY,
                                "fs.oraclebmc.client.retry.timeout.seconds")
                        .put(
                                RETRY_TIMEOUT_RESET_THRESHOLD_IN_SECONDS_KEY,
                                "fs.oraclebmc.client.retry.reset.threshold.seconds")
                        .put(
                                OBJECT_METADATA_CACHING_ENABLED_KEY,
                                "fs.oraclebmc.caching.object.metadata.enabled")
                        .put(
                                OBJECT_METADATA_CACHING_SPEC_KEY,
                                "fs.oraclebmc.caching.object.metadata.spec")
                        .put(
                                JERSEY_CLIENT_LOGGING_ENABLED_KEY,
                                "fs.oraclebmc.client.jersey.logging.enabled")
                        .put(
                                JERSEY_CLIENT_LOGGING_LEVEL_KEY,
                                "fs.oraclebmc.client.jersey.logging.level")
                        .put(
                                JERSEY_CLIENT_LOGGING_VERBOSITY_KEY,
                                "fs.oraclebmc.client.jersey.logging.verbosity")
                        .put(
                                JERSEY_CLIENT_DEFAULT_CONNECTOR_ENABLED_KEY,
                                "fs.oraclebmc.client.jersey.default.connector.enabled")
                        .put(
                                APACHE_MAX_CONNECTION_POOL_SIZE_KEY,
                                "fs.oraclebmc.client.apache.max.connection.pool.size")
                        .put(
                                RENAME_DIRECTORY_NUM_THREADS_KEY,
                                "fs.oraclebmc.rename.operation.numthreads")
                        .put(
                                APACHE_CONNECTION_CLOSING_STRATEGY_KEY,
                                "fs.oraclebmc.client.apache.connection.closing.strategy")
                        .put(
                                OBJECT_AUTO_CLOSE_INPUT_STREAM_KEY,
                                "fs.oraclebmc.object.autoclose.inputstream")
                        .build();

        private static final String FS_OCI_PREFIX = "fs.oci.";

        public static String getDeprecatedKey(String newKey) {
            String oldKey = NEW_TO_OLD_KEYS.get(newKey);
            if (oldKey == null) {
                if (!newKey.startsWith(FS_OCI_PREFIX)) {
                    throw new IllegalArgumentException(
                            "Cannot determine old key for '" + newKey + "'");
                }
                oldKey = "fs.oraclebmc." + newKey.substring(FS_OCI_PREFIX.length());
            }
            return oldKey;
        }
    }
}
