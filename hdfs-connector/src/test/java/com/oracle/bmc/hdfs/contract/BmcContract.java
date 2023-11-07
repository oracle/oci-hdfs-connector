/**
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
 * or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
package com.oracle.bmc.hdfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;

public class BmcContract extends AbstractBondedFSContract {
    public static final String CONTRACT_XML = "contract/oci.xml";
    public static final String CREDENTIALS_XML = "contract/oci-credentials.xml";

    public BmcContract(final Configuration conf) {
        super(conf);
        this.addConfResource(CONTRACT_XML);
        this.addConfResource(CREDENTIALS_XML);
    }

    @Override
    public String getScheme() {
        return "oci";
    }

    public static class InMemory extends BmcContract {
        public static final String CONTRACT_IN_MEMORY_XML = "contract/oci-inmemory.xml";

        public InMemory(Configuration conf) {
            super(conf);
            this.addConfResource(CONTRACT_IN_MEMORY_XML);
        }
    }

    public static class MultipartInMemoryWrite extends BmcContract {
        public static final String CONTRACT_MULTIPART_IN_MEMORY_WRITE_XML =
                "contract/oci-multipartinmemorywrite.xml";

        public MultipartInMemoryWrite(Configuration conf) {
            super(conf);
            this.addConfResource(CONTRACT_MULTIPART_IN_MEMORY_WRITE_XML);
        }
    }

    public static class ReadAhead extends BmcContract {
        public static final String CONTRACT_READ_AHEAD_XML = "contract/oci-readahead.xml";

        public ReadAhead(Configuration conf) {
            super(conf);
            this.addConfResource(CONTRACT_READ_AHEAD_XML);
        }
    }

    public static class PayloadCaching extends BmcContract {
        public static final String CONTRACT_PAYLOAD_CACHING_XML = "contract/oci-payloadcaching.xml";

        public PayloadCaching(Configuration conf) {
            super(conf);
            this.addConfResource(CONTRACT_PAYLOAD_CACHING_XML);
        }
    }

    public static class ApacheClosingStrategy extends BmcContract {
        public static final String CONTRACT_APACHE_CLOSING_STRATEGY_XML =
                "contract/oci-apacheclosingstrategy.xml";

        public ApacheClosingStrategy(Configuration conf) {
            super(conf);
            this.addConfResource(CONTRACT_APACHE_CLOSING_STRATEGY_XML);
        }
    }

    public static class FilesystemCaching extends BmcContract {
        public static final String FILESYSTEM_CACHING_XML = "contract/oci-filesystemcaching.xml";

        public FilesystemCaching(Configuration conf) {
            super(conf);
            this.addConfResource(FILESYSTEM_CACHING_XML);
        }
    }

    public static class StatsMonitorStream extends BmcContract {
        public static final String STATS_MON_STREAM_XML = "contract/oci-statsmonitorstream.xml";

        public StatsMonitorStream(Configuration conf) {
            super(conf);
            this.addConfResource(STATS_MON_STREAM_XML);
        }
    }
}
