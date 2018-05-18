/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs;

import com.oracle.bmc.auth.BasicAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorage;

import static com.oracle.bmc.hdfs.BmcConstants.*;

import com.oracle.bmc.ClientConfiguration;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

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
     */
    IN_MEMORY_WRITE_BUFFER(IN_MEMORY_WRITE_BUFFER_KEY, false),
    /**
     * (boolean, optional) Flag to enable reading all files to memory first (instead streaming from Object Store), to
     * allow faster seeking. See {@link BmcConstants#IN_MEMORY_READ_BUFFER_KEY} for config key name. Default is false.
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
     * (int, optional) The number of threads to use for parallel multi-part uploads. Note, any value <= 0 is interpreted
     * as using the default Java SDK value. A value of 1 means do not use parallel uploads (you can still use multi-part
     * if the "allowed" property is true, but each part will be uploaded serially. This will be one thread per upload).
     * Otherwise, this represents the total number of threads shared amongst all requests (not per request). See
     * {@link BmcConstants#MULTIPART_NUM_UPLOAD_THREADS_KEY} for config key name.
     */
    MULTIPART_NUM_UPLOAD_THREADS(MULTIPART_NUM_UPLOAD_THREADS_KEY, null),
    /**
     * (int, optional) The maximum number of parts to split an object into. Note, any value <= 0 is interpreted as using
     * the default Java SDK value. See {@link BmcConstants#MULTIPART_MAX_PARTS_KEY} for config key name.
     */
    MULTIPART_MAX_PARTS(MULTIPART_MAX_PARTS_KEY, null),
    /**
     * (int, optional) The minimum size, in mebibytes, an object must be before its eligible for multi-part uploads.
     * Note, any value <= 0 is interpreted as using the default Java SDK value. See
     * {@link BmcConstants#MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB_KEY} for config key name.
     */
    MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB(MULTIPART_MIN_SIZE_OF_OBJECT_IN_MB_KEY, null),
    /**
     * (int, optional) The minimum size, in megabytes, each part should be uploaded as. Note, any value <= 0 is
     * interpreted as using the default Java SDK value. See {@link BmcConstants#MULTIPART_MIN_PART_SIZE_IN_MB_KEY} for
     * config key name.
     */
    MULTIPART_MIN_PART_SIZE_IN_MB(MULTIPART_MIN_PART_SIZE_IN_MB_KEY, null),

    /**
     * (boolean, optional) Flag to enable pseudo-streaming to OCI via Multipart Uploads backed by a circular buffer.
     * See {@link BmcConstants#MULTIPART_IN_MEMORY_WRITE_BUFFER_KEY} for config key name. Default is false.
     */
    MULTIPART_IN_MEMORY_WRITE_BUFFER(MULTIPART_IN_MEMORY_WRITE_BUFFER_KEY, false),

    /**
     * (boolean, optional) Flag to enable pseudo-streaming to OCI via Multipart Uploads backed by a circular buffer.
     * See {@link BmcConstants#MULTIPART_IN_MEMORY_WRITE_MAX_INFLIGHT_KEY} for config key name. Default is false.
     */
    MULTIPART_IN_MEMORY_WRITE_MAX_INFLIGHT(MULTIPART_IN_MEMORY_WRITE_MAX_INFLIGHT_KEY, false);

    @Getter private final String propertyName;
    @Getter private final Object defaultValue;
}
