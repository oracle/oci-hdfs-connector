/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;

/**
 * Deprecated implementation of a HDFS {@link FileSystem} that is backed by the BMC Object Store.
 * <p>
 * Filesystems using this store take the URI form: <i>oraclebmc://bucket@namespace</i>. The bucket must be pre-created.
 * <p>
 * Unless otherwise noted, APIs try to follow the specification as defined by:
 * http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/filesystem.html
 *
 * @deprecated Use {@link BmcFilesystem} and the <i>oci://bucket@namespace</i> URI instead
 */
@Slf4j
@Deprecated
public class DeprecatedBmcFilesystem extends BmcFilesystem {
    @Override
    public String getScheme() {
        return BmcConstants.Deprecated.BMC_SCHEME;
    }
}
