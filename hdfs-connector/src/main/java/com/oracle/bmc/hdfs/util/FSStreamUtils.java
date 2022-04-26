/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import javax.ws.rs.ProcessingException;
import java.io.InputStream;

@Slf4j
public class FSStreamUtils {

    /**
     * Closes an <code>InputStream</code> unconditionally.
     * <p>
     * Equivalent to {@link InputStream#close()}, except any exceptions will be ignored.
     *
     * @param stream the InputStream to close, may be null or already closed
     */
    public static void closeQuietly(final InputStream stream) {
        try {
            IOUtils.closeQuietly(stream);
        } catch (ProcessingException e) {
            // Jersey client will throw this when closing a stream that is in an invalid state
            LOG.debug("Error closing stream", e);
        }
    }
}
