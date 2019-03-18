/**
 * Copyright (c) 2016, 2019, Oracle and/or its affiliates. All rights reserved.
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

    /**
     * This class contains constants with deprecated values. The HDFS connector will first try the current values
     * in {@link com.oracle.bmc.hdfs.BmcConstants}.
     */
    public enum Deprecated {
        ; // no-value enum to prevent instantiation
        public static final String BMC_SCHEME = "oraclebmc";

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
                        .build();

        public static String getDeprecatedKey(String newKey) {
            return NEW_TO_OLD_KEYS.get(newKey);
        }
    }
}
