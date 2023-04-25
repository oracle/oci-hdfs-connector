/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.store;

import com.oracle.bmc.hdfs.util.BiFunction;
import com.oracle.bmc.objectstorage.transfer.UploadManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * {@link OutputStream} abstract class for custom write stream for users to configure
 * their own streams. Since the constructor is invoked using reflection, subclasses are not allowed
 * to modify the constructor
 */
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractBmcCustomOutputStream extends OutputStream {
    private final BmcPropertyAccessor propertyAccessor;
    private final UploadManager uploadManager;
    private final int bufferSizeInBytes;
    private final BiFunction<Long, InputStream, UploadManager.UploadRequest> requestBuilderFn;
}
