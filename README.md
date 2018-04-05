# Oracle Cloud Infrastructure HDFS Connector for Object Storage
[![Build Status](https://travis-ci.org/oracle/oci-hdfs-connector.svg?branch=master)](https://travis-ci.org/oracle/oci-hdfs-connector)

## About

oci-hdfs-connector provides the public HDFS connector that integrates with the Oracle Cloud Infrastructure Object Storage Service.

The project is open source and maintained by Oracle Corp. The home page for the project is [here](https://docs.us-phoenix-1.oraclecloud.com/Content/API/SDKDocs/hdfsconnector.htm).

## Installation

See [the documentation](https://docs.us-phoenix-1.oraclecloud.com/Content/API/SDKDocs/hdfsconnector.htm) for details.

## Examples

Examples can be found [here](https://github.com/oracle/oci-hdfs-connector/blob/master/hdfs-example/src/main/java/com/oracle/bmc/hadoop/example/SampleOracleBmcHadoopJob.java).

## Documentation

Full documentation, including prerequisites and installation and configuration instructions, can be found [here](https://docs.us-phoenix-1.oraclecloud.com/Content/API/SDKDocs/hdfsconnector.htm).

API reference can be found [here](https://docs.us-phoenix-1.oraclecloud.com/tools/hdfs/latest/).

## Help

See the “Questions or Feedback?” section [here](https://docs.us-phoenix-1.oraclecloud.com/Content/API/SDKDocs/hdfsconnector.htm#questions).

## Version

The first three numbers of HDFS connector version are Hadoop version that the connector was compiled and tested against.

## Build

Building HDFS connector relies on Maven artifacts that are provided by the Java SDK. To obtain the artifacts, you must [download the Java SDK](https://github.com/oracle/oci-java-sdk/) and build it locally. You can then build the HDFS connector.
 
Important: The Java SDK file version that you download from the [Oracle Releases page](https://github.com/oracle/oci-java-sdk/releases) must match the HDFS connector version, which you can find in the [hdfs-connector/pom.xml](/blob/8cd12e68d27e1c76b01abafccb0bcc795d0a8e04/hdfs-connector/pom.xml#L110) file in the <dependency> tag block that has the groupId attribute com.oracle.oci.sdk.

## Changes

See [CHANGELOG](/CHANGELOG.md).

## Contributing

oci-hdfs-connector is an open source project. See [CONTRIBUTING](/CONTRIBUTING.md) for details.

Oracle gratefully acknowledges the contributions to oci-hdfs-connector that have been made by the community.

## Known Issues

You can find information on any known issues with the connector [here](https://docs.us-phoenix-1.oraclecloud.com/Content/knownissues.htm) and under the “Issues” tab of this GitHub repository.

## License

Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.

This SDK and sample is dual licensed under the Universal Permissive License 1.0 and the Apache License 2.0.

See [LICENSE](/LICENSE.txt) for more details.
