/**
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.contract.deprecated;

import com.oracle.bmc.hdfs.BmcConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

import java.util.Map;
import java.util.regex.Pattern;

public class DeprecatedBmcContract extends AbstractBondedFSContract {
    public static final String CONTRACT_XML = "contract/oci.xml";
    public static final String CREDENTIALS_XML = "contract/oci-credentials-deprecated.xml";

    public DeprecatedBmcContract(final Configuration conf) {
        super(conf);
        this.addConfResource(CONTRACT_XML);
        this.addConfResource(CREDENTIALS_XML);

        // convertToDeprecatedConfiguration(conf);
    }

    public static void convertToDeprecatedConfiguration(Configuration conf) {
        // get all the configuration items that have ".oci" in the key
        Map<String, String> ociConfig =
                conf.getValByRegex(".*\\." + BmcConstants.OCI_SCHEME + ".*");
        for (Map.Entry<String, String> e : ociConfig.entrySet()) {
            // replace ".oci" with the deprecated ".oraclebmc"
            String deprecatedKey =
                    e.getKey()
                            .replaceAll(
                                    "\\." + BmcConstants.OCI_SCHEME,
                                    "." + BmcConstants.Deprecated.BMC_SCHEME);
            conf.set(deprecatedKey, e.getValue());
            // remove new item; we only allow either the new or the deprecated item to be set
            conf.unset(e.getKey());
        }

        // get the file system URI and replace the scheme
        String ociUri = conf.get("fs.contract.test.fs." + BmcConstants.Deprecated.BMC_SCHEME);
        if (ociUri != null) {
            String bmcUri =
                    ociUri.replaceFirst(
                            BmcConstants.OCI_SCHEME + "://",
                            BmcConstants.Deprecated.BMC_SCHEME + "://");
            conf.set("fs.contract.test.fs." + BmcConstants.Deprecated.BMC_SCHEME, bmcUri);
        }
    }

    @Override
    public String getScheme() {
        return "oraclebmc";
    }
}
