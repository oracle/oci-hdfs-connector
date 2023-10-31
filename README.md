# Oracle Cloud Infrastructure HDFS Connector for Object Storage

## About

oci-hdfs-connector provides the public HDFS connector that integrates with the Oracle Cloud Infrastructure Object Storage Service.

The project is open source and maintained by Oracle Corp. The home page for the project is [here](https://docs.cloud.oracle.com/Content/API/SDKDocs/hdfsconnector.htm).

## Installation

See [the documentation](https://docs.cloud.oracle.com/Content/API/SDKDocs/hdfsconnector.htm) for details.

## Examples

Examples can be found [here](https://github.com/oracle/oci-hdfs-connector/blob/master/hdfs-example/src/main/java/com/oracle/bmc/hadoop/example/SampleOracleBmcHadoopJob.java).

## Documentation

Full documentation, including prerequisites and installation and configuration instructions, can be found [here](https://docs.cloud.oracle.com/Content/API/SDKDocs/hdfsconnector.htm).

HDFS Connector provides support for delegation token configurator that injects a delegation token on every request. This can be done by setting the delegation token file path in an environment variable `OCI_DELEGATION_TOKEN_FILE` or by setting the Hadoop property `fs.oci.delegation.token.filepath`

API reference can be found [here](https://docs.cloud.oracle.com/tools/hdfs/latest/).

## Help

See the “Questions or Feedback?” section [here](https://docs.cloud.oracle.com/Content/API/SDKDocs/hdfsconnector.htm#questions).

## Version

The first three numbers of HDFS connector version represent the Hadoop version that the connector was compiled and tested against. The next three numbers are for the platform, major and minor versions respectively.
* Minor version is incremented when there are no breaking changes
* Major version is incremented when there are breaking changes which might affect some users
* Platform version is incremented when there are bigger important breaking changes which would affect many users

For example, in HDFS connector version `3.3.1.x.y.z`, `3.3.1` is for the Hadoop version being used and `x.y.z` refers to the {platform}.{major}.{minor} version of the HDFS connector.

## Build

Building HDFS connector relies on Maven artifacts that are provided by the Java SDK. To obtain the artifacts, you must [download the Java SDK](https://github.com/oracle/oci-java-sdk/) and build it locally. You can then build the HDFS connector.
 
Important: The Java SDK file version that you download from the [Oracle Releases page](https://github.com/oracle/oci-java-sdk/releases) must match the HDFS connector version, which you can find in the [hdfs-connector/pom.xml](/blob/8cd12e68d27e1c76b01abafccb0bcc795d0a8e04/hdfs-connector/pom.xml#L110) file in the <dependency> tag block that has the groupId attribute com.oracle.oci.sdk.

## Changes

See [CHANGELOG](/CHANGELOG.md).

## Contributing

oci-hdfs-connector is an open source project. See [CONTRIBUTING](/CONTRIBUTING.md) for details.

Oracle gratefully acknowledges the contributions to oci-hdfs-connector that have been made by the community.

## Known Issues

You can find information on any known issues with the connector [here](https://docs.cloud.oracle.com/Content/knownissues.htm) and under the “Issues” tab of this GitHub repository.
  You can find information on any known issues with the SDK [here](https://docs.cloud.oracle.com/iaas/Content/knownissues.htm) and under the [“Issues” tab of this GitHub repository](https://github.com/oracle/oci-hdfs-connector/issues).

### Potential data corruption issue for OCI HDFS Connector with `RefreshableOnNotAuthenticatedProvider`

**Details**: If you are using version 3.2.1.1 or earlier of the OCI HDFS Connector and you use a `RefreshableOnNotAuthenticatedProvider` (e.g. `InstancePrincipalsCustomAuthenticator`, or generally for Resource Principals or Instance Principals) you may be affected by **silent data corruption**.

**Workaround**: Update the OCI HDFS Connector to version 3.2.1.3 or later. For more information about this issue and workarounds, see [Potential data corruption issue for OCI HDFS Connector with `RefreshableOnNotAuthenticatedProvider`](https://github.com/oracle/oci-hdfs-connector/issues/35).

**Direct link to this issue**: [Potential data corruption issue with OCI HDFS Connector on binary data upload with `RefreshableOnNotAuthenticatedProvider`](https://docs.cloud.oracle.com/en-us/iaas/Content/knownissues.htm#knownissues_topic_Potential_data_corruption_with_OCI_Java_SDK_on_binary_data_upload_with_RefreshableOnNotAuthenticatedProvider_HDFS)

## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](./CONTRIBUTING.md)

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process

## License

Copyright (c) 2016, 2023, Oracle and/or its affiliates.  All rights reserved.
This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl
or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.

See [LICENSE](/LICENSE.txt) for more details.
