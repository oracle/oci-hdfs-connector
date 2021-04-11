/**
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.  All rights reserved.
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

    public static final String MULTIPART_IN_MEMORY_WRITE_BUFFER_KEY = "fs.oci.io.write.multipart.inmemory";

    public static final String MULTIPART_IN_MEMORY_WRITE_MAX_INFLIGHT_KEY =
            "fs.oci.io.write.multipart.inmemory.maxinflight";

    public static final String READ_AHEAD_KEY = "fs.oci.io.read.ahead";

    public static final String READ_AHEAD_BLOCK_SIZE_KEY = "fs.oci.io.read.ahead.blocksize";

    public static final String OBJECT_STORE_CLIENT_CLASS_KEY = "fs.oci.client.custom.client";

    public static final String OBJECT_STORE_AUTHENTICATOR_CLASS_KEY =
            "fs.oci.client.custom.authenticator";

    public static final String HOST_NAME_KEY = "fs.oci.client.hostname";

    public static final String TENANT_ID_KEY = "fs.oci.client.auth.tenantId";

    public static final String USER_ID_KEY = "fs.oci.client.auth.userId";

    public static final String FINGERPRINT_KEY = "fs.oci.client.auth.fingerprint";

    public static final String PEM_FILE_PATH_KEY = "fs.oci.client.auth.pemfilepath";

    public static final String PASS_PHRASE_KEY = "fs.oci.client.auth.passphrase";

    public static final String CONNECTION_TIMEOUT_MILLIS_KEY =
            "fs.oci.client.http.connectiontimeout";

    public static final String READ_TIMEOUT_MILLIS_KEY = "fs.oci.client.http.readtimeout";

    public static final String MULTIPART_ALLOWED_KEY = "fs.oci.client.multipart.allowed";

    public static final String MULTIPART_NUM_UPLOAD_THREADS_KEY =
            "fs.oci.client.multipart.numthreads";

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
                        .put(
                                CONNECTION_TIMEOUT_MILLIS_KEY,
                                "fs.oraclebmc.client.http.connectiontimeout")
                        .put(READ_TIMEOUT_MILLIS_KEY, "fs.oraclebmc.client.http.readtimeout")
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
                        .put(OBJECT_METADATA_CACHING_ENABLED_KEY,
                             "fs.oraclebmc.caching.object.metadata.enabled")
                        .put(OBJECT_METADATA_CACHING_SPEC_KEY,
                             "fs.oraclebmc.caching.object.metadata.spec")
                        .put(JERSEY_CLIENT_LOGGING_ENABLED_KEY,
                                "fs.oraclebmc.client.jersey.logging.enabled")
                        .put(JERSEY_CLIENT_LOGGING_LEVEL_KEY,
                                "fs.oraclebmc.client.jersey.logging.level")
                        .put(JERSEY_CLIENT_LOGGING_VERBOSITY_KEY,
                                "fs.oraclebmc.client.jersey.logging.verbosity")
                        .build();

        private static final String FS_OCI_PREFIX = "fs.oci.";

        public static String getDeprecatedKey(String newKey) {
            String oldKey = NEW_TO_OLD_KEYS.get(newKey);
            if (oldKey == null) {
                if (!newKey.startsWith(FS_OCI_PREFIX)) {
                    throw new IllegalArgumentException("Cannot determine old key for '" + newKey + "'");
                }
                oldKey = "fs.oraclebmc." + newKey.substring(FS_OCI_PREFIX.length());
            }
            return oldKey;
        }
    }
}
