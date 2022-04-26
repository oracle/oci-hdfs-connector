/**
 * Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs;

import static org.junit.Assert.*;
import org.junit.Test;

import java.net.URI;

public class BmcFilesystemTest {

    @Test
    public void parseNormalUri() {
        final URI uri = URI.create("oci://bucket@namespace:2343/a/b/c/d.json");
        final BmcFilesystem.UriParser uriParser = new BmcFilesystem.UriParser(uri);
        assertEquals("oci", uriParser.getScheme());
        assertEquals("bucket", uriParser.getBucket());
        assertEquals("namespace", uriParser.getNamespace());
        assertEquals("bucket@namespace:2343", uriParser.getAuthority());
    }

    @Test
    public void parseUnderscoreUri() {
        final URI uri = URI.create("oraclebmc://bucket_1_2_3@name_space_1:3002/x/y/z");
        final BmcFilesystem.UriParser uriParser = new BmcFilesystem.UriParser(uri);
        assertEquals("oraclebmc", uriParser.getScheme());
        assertEquals("bucket_1_2_3", uriParser.getBucket());
        assertEquals("name_space_1", uriParser.getNamespace());
        assertEquals("bucket_1_2_3@name_space_1:3002", uriParser.getAuthority());
    }
}
