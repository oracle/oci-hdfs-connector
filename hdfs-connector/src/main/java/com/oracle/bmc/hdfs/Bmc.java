/**
 * Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.
 */
package com.oracle.bmc.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

/**
 * Oracle Cloud Infrastructure implementation of AbstractFileSystem. Delegates to BmcFilesystem.
 * <p>
 * To enable, set:
 *
 * <pre>
 * &lt;property&gt;
 *   &lt;name&gt;fs.AbstractFileSystem.oci.impl&lt;/name&gt;
 *   &lt;value&gt;com.oracle.bmc.hdfs.Bmc&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class Bmc extends DelegateToFileSystem {

    public Bmc(URI theUri, Configuration conf) throws IOException, URISyntaxException {
        super(theUri, new BmcFilesystem(), conf, BmcConstants.OCI_SCHEME, false);
    }

    @Override
    public int getUriDefaultPort() {
        return BmcConstants.DEFAULT_PORT;
    }
}
