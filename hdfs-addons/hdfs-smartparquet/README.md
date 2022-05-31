# OCI HDFS Connector Smart Parquet Add-On

## About

The oci-hdfs-addons-smartparquet is an optional add-on to the OCI HDFS Connector.
The addon provides smart parquet caching features for the OCI HDFS Connector

## Installation
1. The HDFS Connector must be installed and configured before installing the add-on.
2. Copy the supplied oci-hdfs-addons-smartparquet and third-party jar files to your application's classpath.

## Configuration
Set the value of `fs.oci.io.read.custom.stream` HDFS connector property in the `core-site.xml` file to the following :
`com.oracle.bmc.hdfs.addons.smartparquet.BmcSmartParquetFSInputStream`

## Example
Set the following property in the `core-site.xml` file :

`<fs.oci.io.read.custom.stream>com.oracle.bmc.hdfs.addons.smartparquet.BmcSmartParquetFSInputStream</fs.oci.io.read.custom.stream>`

## License
Copyright (c) 2016, 2022, Oracle and/or its affiliates.  All rights reserved.
This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

See [LICENSE](../../LICENSE.txt) for more details.