# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).
## 3.4.1.0.0.1 - 2025-01-23
### Added
- Introduced `fs.oci.io.write.allow.overwrite` property to manage overwrite behavior during write operations.
- Added support for using CRC32c (composite-crc32c) during file uploads via the HDFS Connector. Users can enable this by setting `fs.checksum.combine.mode` property in `core-site.xml` to `COMPOSITE_CRC`.
- Implemented `getFileChecksum()` API to provide the CRC32c checksum of an object, supporting data integrity verification for DistCP operations.

### Removed
- Removed the deprecated `fs.oci.io.write.multipart.overwrite` property in favor of the newly introduced `fs.oci.io.write.allow.overwrite` property.

### Changed
- Updated OCI Java SDK to version `3.55.0` to enable checksum support. This update also removes BouncyCastle dependencies, and changes have been made in the connector to ensure compatibility.

### Fixed
- Addressed potential issues with internal retries in the HDFS Connector's output stream. The new retrier ensures that retries do not result in a `412 Precondition Failed` error when the application assumes the object does not already exist.
- Retrier behavior is now aligned with the `allowoverwrite` property passed from the `OpenOutputStream` method to API calls.
  

## 3.4.1.0.0.0 - 2024-12-13
### Changed
- Updated Hadoop version from `3.3.4` to `3.4.1` to resolve security vulnerabilities ([GHSA-f5fw-25gw-5m92](https://github.com/advisories/GHSA-f5fw-25gw-5m92)).
- Updated OCI Java SDK version from `3.39.0` to `3.50.0` to address security vulnerabilities ([GHSA-v435-xc8x-wvr9](https://github.com/advisories/GHSA-v435-xc8x-wvr9)).
- Updated Netty dependencies from `4.1.86.Final` to `4.1.115.Final` to resolve vulnerabilities ([GHSA-5jpm-x58v-624v](https://github.com/advisories/GHSA-5jpm-x58v-624v)).
- Updated Jetty dependencies from `9.4.44.v20210927` to `9.4.56.v20240826` to address security vulnerabilities ([GHSA-58qw-p7qm-5rvh](https://github.com/advisories/GHSA-58qw-p7qm-5rvh)).
- Updated OkHttp from `4.9.3` to `4.12.0` to resolve vulnerabilities ([GHSA-w33c-445m-f8w7](https://github.com/advisories/GHSA-w33c-445m-f8w7)).
- Updated Apache Avro from `1.11.3` to `1.11.4` to resolve vulnerabilities ([GHSA-r7pg-v2c8-mfg3](https://github.com/advisories/GHSA-r7pg-v2c8-mfg3)).
### Fixed
- Fixed `BmcFileSystemImpl` to enhance resource management by improving close operations and cache invalidation.
### Added
- Added configuration `fs.oci.client.circuitbreaker.enabled` to enable or disable the circuit breaker.

## 3.3.4.1.5.1 - 2024-08-27
### Changed
-  Updated commons-compress from `1.21` to `1.26.0`,  CVE-2024-26308
### Added
-  Added jettison as dependency for HDFS-connector fat jar
-  Added SECURITY.md

## 3.3.4.1.5.0 - 2024-06-17
### Changed
-  Updated OCI Java SDK version to `3.39.0`
-  Updated org.bouncycastle from `jdk15on` to `jdk15to18`
### Added
-  Added read retry policy to InputStream

## 3.3.4.1.4.2 - 2024-03-12
### Changed
-  Updated OCI Java SDK version to `3.34.0`
-  Updated com.fasterxml.jackson.core:jackson-databind to `2.16.0`
-  Updated org.apache.avro.avro to `1.11.3`
-  Use org.apache.commons.commons-text `2.16.0`
-  Shaded io.netty packages

### Fixed
-  Fixed external kinit OID issue and refactored SpnegoGenerator
-  Fixed contract test failure AbstractContractSeekTest.testReadFullyZeroByteFile
-  Fixed contract test failure AbstractContractSeekTest.testSeekReadClosedFile

## 3.3.4.1.4.1 - 2024-02-06
### Added
- Added custom authenticator which uses OKE Workload Identity authentication

### Changed
- Improved logs in InputStreams

## 3.3.4.1.4.0 - 2023-12-01
### Added
- Added support to Kerberos authentication with SPNEGO token 

## 3.3.4.1.3.0 - 2023-11-07
### Added
- Added observability feature that generates metrics pertaining to operations conducted through the connector, such as reading, writing, and deleting data. 

## 3.3.4.1.2.1 - 2023-10-20
### Added
- Add multi-region support for the same configuration.  This feature can be enabled by setting `fs.oci.client.multiregion.enabled` to true. Once enabled, the user has the option to append a region code or ID after the namespace in the `oci://<bucket>@<namespace>.<region>/file` format. This action will result in the creation of a dedicated BmcFilesystem instance for the specified region. Your applications can then make use of different URIs to create BmcFilesystem instances, each directed towards distinct endpoints.

### Changed
- Removed unnecessary HeadObject requests on directory objects for getFileStatus
- Enhanced object creation process by eliminating redundant ListObjects requests

### Fixed
- Fixed the problem that resulted in the premature termination of reading a single byte from an object using BmcParallelReadAheadFSInputStream

## 3.3.4.1.2.0 - 2023-06-22
### Added
- Added support for namespace-prefixed domains in the Object Storage service

### Changed
- Updated OCI Java SDK version to `3.17.1`
- Updated `guava` version from `30.1-jre` to `32.0.1-jre`
- Replaced LinkedBlockingQueue with SynchronousQueue to hand off tasks to the executor
- Added relocation for shaded package `javax.servlet`

### Fixed
- Fixed the issue that caused object loss when performing a renaming operation, in case the target already existed

## 3.3.4.1.1.0 - 2023-05-09
### Added
- Added support for parallel ranged GET requests in read-ahead mode

### Changed
- Optimized recursive calls for list files and delete path
- Optimized implementation for `FileSystem.getContentSummary` 
- Replaced multipart request with PutObject for small object writes in `BmcMultipartOutputStream`

## 3.3.4.1.0.0 - 2023-04-25
### Added
- Added support for OCI Java SDK 3.x
- Added support for parameterized and realm-specific endpoint templates
- Added support for calculating part MD5s on a separate executor in BmcMultipartOutputStream

### Changed
- Updated OCI Java SDK version to `3.12.1`
- Updated `json-smart` from `2.4.7` to `2.4.9`

### Fixed
- Fixed race when concurrently creating objects with BmcMultipartOutputStream
- Fixed the destination dir containing `$` character for renameDir

## 3.3.4.0.1.1 - 2023-03-14
### Changed
- Updated `io.netty:netty-codec` from version `4.1.77.Final` to `4.1.86.Final`

### Fixed
- Fixed the `createFileStatus` to use the `timeModified` instead of `timeCreated`. This bug caused the `LocatedFileStatus` to have wrong `modification_time` when objects are overwritten in OCI Object Storage.
- Fixed the multipart upload default configuration to use the correct upload size of 128 MiB

## 3.3.4.0.1.0 - 2022-11-08
### Changed
- Updated OCI Java SDK version to `2.47.0`
- Updated `com.fasterxml.woodstox:woodstox-core` from version `6.2.3` to `6.4.0`
- Updated `com.fasterxml.jackson.core:jackson-databind` from version `2.12.6.1` to `2.13.4.2`

## 3.3.4.0.0.0 - 2022-10-04
### Added
- Added support for delegation token to HDFS connector. This feature can be enabled by setting the property `fs.oci.delegation.token.filepath` to the path of file having the delegation token.

### Changed
- Updated to Hadoop version 3.3.4
- Updated to OCI Java SDK version 2.38.0

### Fixed
- Fixed multipart upload to use the correct upload size
- Fixed BmcFilesystem cache to correctly manage the items
- Fixed NullPointerException when default filesystem is oci
- Fixed IOException in ReadAheadFileInputStream to read all bytes in a parquet file
- Fixed OCI HDFS connector in read-ahead mode doesn't emit bytesRead input metric [#77](https://github.com/oracle/oci-hdfs-connector/issues/77)
- Fixed HDFS connector issue for not being able to use smart parquet add-on

## 3.3.1.0.3.6 - 2022-06-07
### Fixed
- Fixed oci-hdfs jar file to not contain class files from `hadoop-common` and `hadoop-hdfs`

## 3.3.1.0.3.5 - 2022-05-31
### Added
- Added support for specifying custom read stream class. This feature can be enabled by setting the property `fs.oci.io.read.custom.stream` to the name of the custom read stream class.
- Added support for specifying custom write stream class. his feature can be enabled by setting the property `fs.oci.io.write.custom.stream` to the name of the custom write stream class.
- Added support for smart parquet add-on feature

## 3.3.1.0.3.4 - 2022-05-03
### Fixed
- Fixed non-daemon threads preventing JVM shutdown when `fs.oci.rename.operation.numthreads` was set to use a single thread for renaming.

## 3.3.1.0.3.3 - 2022-04-26
### Fixed
- Fixed ArrayIndexOutOfBoundsException in read-ahead mode [#69](https://github.com/oracle/oci-hdfs-connector/issues/69)

### Added
- Added support for Resource Principals Authentication v2.2

### Changed
- Added relocation for shaded packages `org.objectweb`

## 3.3.1.0.3.2 - 2022-01-25
### Added
- Added support for caching `BmcFilesystem` instances. This feature can be enabled by setting `fs.oci.caching.filesystem.enabled` to `true. If enabled, the properties `fs.oci.caching.filesystem.maxsize.count` and `fs.oci.caching.filesystem.initialcapacity.count` control the size of the cache, while either `fs.oci.caching.filesystem.expireafteraccess.seconds` or `fs.oci.caching.filesystem.expireafterwrite.seconds` control the expiration. If a `BmcFilesystem` instance exists in the cache with the same URI and configuration, the instance will be re-used, leading to performance improvements at the cost of the cache's memory footprint.

## 3.3.1.0.3.1 - 2022-01-06
### Changed
- Updated `log4j` dependencies to version `2.17.1` to address CVE-2021-44832

## 3.3.1.0.3.0 - 2021-12-20
### Changed
- Updated `log4j` dependencies to version `2.17.0` to address CVE-2021-45105

## 3.3.1.0.2.0 - 2021-12-15
### Changed
- Updated `log4j` dependencies to version `2.16.0` to address CVE-2021-45046

## 3.3.1.0.1.0 - 2021-12-14
### Changed
- Updated to OCI Java SDK version 2.11.1
- Updated `log4j` dependencies to version `2.15.0` to address CVE-2021-44228
- Removed dependencies `log4j-core`, `log4j-slf4j-impl`, `log4j-1.2-api` by default . To include these dependencies, run `mvn` 
with option `-Duse-slf4j-log4j`

## 3.3.1.0.0.1 - 2021-10-28
### Changed
- Updated to OCI Java SDK version 2.7.1

### Fixed
- Updated Jetty version to `9.4.44` from `11.0.6` (minimum requirement of Java 11) to add back support for older versions of Java

## 3.3.1.0.0.0 - 2021-08-31
### Added
- Added support for disabling auto-close of object streams that are obtained through `getObject` operation. This can be disabled by setting `fs.oci.object.autoclose.inputstream` to `false`. If disabled, the streams obtained through `getObject` that are completely read will not be closed automatically. If not specified, this option will be enabled by default.

### Changed
- Updated to Hadoop version 3.3.1

## 3.3.0.7.0.1 - 2021-06-29
### Added
- Added support for multi-part upload streaming using a finite-sized in-memory buffer. This mode can be enabled by setting `fs.oci.io.write.multipart.inmemory` to `true`. If enabled, `fs.oci.client.multipart.numthreads` should be set to the number of parallel threads (greater than 1). `fs.oci.io.write.multipart.inmemory` cannot be enabled at the same time as `fs.oci.io.write.inmemory`. `fs.oci.io.write.multipart.overwrite` controls whether objects are allowed to be overwritten (default is `false`). `fs.oci.io.write.multipart.inmemory.tasktimeout.seconds` sets the number of seconds before giving up for upload tasks that cannot start because the maximum number of parallel threads has been reached (default is `900`, meaning 15 minutes).

## 3.3.0.7.0.0 - 2021-06-22
### Added
- Added support for changing to Jersey default `HttpUrlConnectorProvider` for sending HTTP requests using the `fs.oci.client.jersey.default.connector.enabled` configuration key
- Added support for changing the maximum number of connections in the connection pool when using the Apache Connector for sending HTTP requests using the `fs.oci.client.apache.max.connection.pool.size` configuration key
- Added support for changing the connection closing strategy when using the Apache Connector for sending HTTP requests using the `fs.oci.client.apache.connection.closing.strategy` configuration key
- Added support for parallel `renameDirectory` operation and support for changing the number of threads when performing the `renameDirectory` operation using the `fs.oci.rename.operation.numthreads` configuration key
- Added support for Resource Principals Authentication
- Added support for more ways to build object storage endpoint by using the `fs.oci.client.regionCodeOrId` configuration key and by using instance metadata

### Changed
- Updated to OCI Java SDK version 2.0.0
- Usage of Jersey's `ApacheConnectorProvider` by default for sending HTTP requests
- Performance issues due to upgrade to Jersey's `ApacheConnectorProvider` by default. For changing back to Jersey default `HttpUrlConnectorProvider` and other performance enhancements, look into `BmcProperties`

### Fixed
- `com.oracle.bmc.hdfs.store.BmcReadAheadFSInputStream read()` now correctly identifies EOF

## 3.3.0.5 - 2021-05-04
- Added payload caching using the `fs.oci.caching.object.payload.enabled` key.
  - This is an on-disk cache that stores payloads in the directory indicated by`fs.oci.caching.object.payload.directory`.
  - The cache size can be configured using `fs.oci.caching.object.payload.maxweight.bytes` or `fs.oci.caching.object.payload.maxsize.count` (mutually exclusive).
  - The property `fs.oci.caching.object.payload.initialcapacity.count` controls the initial cache size.
  - The cache's eviction policy is controlled using `fs.oci.caching.object.payload.expireafteraccess.seconds` or `fs.oci.caching.object.payload.expireafterwrite.seconds` (mutually exclusive).
  - A consistency policy can be set using `fs.oci.caching.object.payload.consistencypolicy.class`. The default is `com.oracle.bmc.hdfs.caching.StrongConsistencyPolicy`, which ensures that the cache is consistent with objects in Object Storage (if an object is changed in Object Storage after it was cached, the cached item will be evicted, and the object will be loaded from Object Storage). If you know your data is immutable, you can set this property to `com.oracle.bmc.hdfs.caching.NoOpConsistencyPolicy`, which does not check for consistency, therefore reducing the number of requests by a factor of two.
  - It is important to (a) close streams read from HDFS, (b) read the streams to their end, or (c) allow those streams to be garbage-collected by the Java runtime. Otherwise, cached items may remain on disk even after they have been evicted.

## 3.3.0.4 - 2021-04-27
- Updated transitive Jetty dependencies to 9.4.40.v20210413 to address CVE-2021-28165.

## 3.3.0.3 - 2021-04-02
- Updated to OCI Java SDK version 1.35.0

## 3.3.0.0 - 2021-03-16
### Added
- Added metadata caching using the `fs.oci.caching.object.metadata.enabled` and `fs.oci.caching.object.metadata.spec` configuration keys. Note that there is no check for consistency, and if your data in Object Storage changes, the cache may return outdated data. Therefore, it is most appropriate when your data is read-only and does not change. Use caution when applying these settings.
- Added read-ahead and parquet caching. The read-ahead feature is configured using `fs.oci.io.read.ahead` and `fs.oci.io.read.ahead.blocksize`. Parquet caching, which requires `fs.oci.io.read.ahead=true`, is controlled using `fs.oci.caching.object.parquet.enabled` and `fs.oci.caching.object.parquet.spec`. Note that there is no check for consistency, and if your data in Object Storage changes, the cache may return outdated data. Therefore, it is most appropriate when your data is read-only and does not change. Use caution when applying these settings.
- Added Jersey client logging, configured using `fs.oci.client.jersey.logging.enabled`, `fs.oci.client.jersey.logging.level`, and `fs.oci.client.jersey.logging.verbosity`.

### Changed
- Updated to Hadoop version 3.3.0
- Updated to OCI Java SDK version 1.33.1

## 3.2.1.3 - 2020-11-03
### Changed
- Updated to OCI Java SDK version 1.25.2

### Fixed
- Fixed a potential data curruption problem with `RefreshableOnNotAuthenticatedProvider`. We recommend that you update to this version 3.2.1.3 or later. For details, see https://github.com/oracle/oci-hdfs-connector/issues/35

## 3.2.1.1 - 2020-09-15
### Changed
- Updated to OCI Java SDK version 1.23.1

## 3.2.1.0 - 2020-09-01
### Changed
- Updated to Hadoop version 3.2.1 
- Updated to OCI Java SDK version 1.22.1

## 2.9.2.7 - 2020-08-11
### Added
- Release incorporates `hdfs-full` module.

### Changed
- Updated to OCI Java SDK version 1.22.0

## 2.9.2.6 - 2020-06-02
### Changed
- Updated to OCI Java SDK version 1.17.5

## 2.9.2.5 - 2020-05-05
### Changed
- Updated to OCI Java SDK version 1.17.0

### Added
- Added DelayStrategy that resets the exponential backoff between retries after reaching a maximum time, configuratble using `fs.oraclebmc.client.retry.reset.threshold.seconds`

## 2.9.2.4 - 2019-03-12
### Changed
- Updated to Java SDK version 1.14.0

## 2.9.2.1 - 2019-08-27
### Changed
- Updated to Java SDK version 1.6.2

## 2.9.2.0 - 2019-08-13
### Changed
- Updated version number to stem from Hadoop version 2.9.2

### Fixed
- Fix race condition in `BmcFileBackedOutputStream#createBufferFile`

## 2.7.7.3 - 2019-07-09
### Added
- Support for retries upon failures. Retry timeout is configurable via `fs.oci.client.retry.timeout.seconds`

### Changed
- Updated to Java SDK version 1.5.12

## 2.7.7.2 - 2019-03-19
### Fixed
- BmcDirectFSInputStream#read now attempts to retry the read from the service when an IOException is thrown

### Changed
- Updated to Java SDK version 1.4.2

## 2.7.7.1 - 2018-12-13
### Changed
- Added relocation for shaded packages `javax.annotation`, `javax.validation` and `javax.inject`

## 2.7.7.0 - 2018-11-01
### Changed
- Updated version number to stem from Hadoop version 2.7.7
- Updated to latest Java SDK (1.2.49) to leverage the updated Object Storage UploadManager with HTTP proxy support
- The configuration option of `MULTIPART_MIN_PART_SIZE_IN_MB` is now deprecated in favor of `MULTIPART_PART_SIZE_IN_MB` to correspond with the configuration changes for the UploadManager in the java SDK
- Bouncy castle and JSR-305 jars are no longer bundled within the distribution jar and now must be included in the Hadoop CLASSPATH.  Required third party jars are bundled under the `third-party/lib` folder of the distribution zip archive

### Added
- Support for configuring an HTTP proxy.  More information can be found [here](https://docs.cloud.oracle.com/iaas/Content/API/SDKDocs/hdfsconnector.htm)

## 2.7.2.2 - 2018-07-12
### Fixed
- Disabled caching of stale key id and private key in the `InstancePrincipalsCustomAuthenticator` class

### Changed
- Updated to latest Java SDK (1.2.42) to pick up bug fixes

## 2.7.2.1 - 2018-06-28
### Fixed
- Enabled progress reporting to Application Master during upload operation
- Enabled usage in a Hadoop deployment with kerberos

### Changed
- Updated to latest Java SDK (1.2.41) to pick up bug fixes

## 2.7.2.0 - 2018-04-05
### Fixed
- Added build instruction and fixed broken GitHub links in README

### Changed
- Updated version number to stem from Hadoop version 2.7.2

## 1.2.10 - 2018-03-27
### Changed
- Release to [GitHub](https://github.com/oracle/oci-hdfs-connector)

### Added
- Support instance principals authentication

## 1.2.7 - 2017-12-11
### Changed
- Replaced copy+delete rename operation with renameObject to improve performance

## 1.2.6 - 2017-11-27
### Fixed
- Fetching the private key password now uses 'getPassword' from the Configuration instead of getting the string in plaintext

### Added
- Added ability to override configuration based on bucket and namespace being accessed

## 1.2.5 - 2017-09-11
### Changed
- Maven packages renamed from "oracle-bmc-*" to "oci-*"" (group id renamed from "com.oracle.bmc.sdk" to "com.oracle.oci.sdk")
- Renamed configuration properties (from "*oraclebmc*" to "*oci*"); old properties are deprecated (see "Deprecated" below).
- Renamed HDFS scheme (from "oraclebmc" to "oci"); old scheme is deprecated (see "Deprecated" below).
- HTTP user agent changed from "Oracle-BMC_HDFS_Connector/<version>" to "Oracle-HDFS_Connector/<version>"

### Deprecated
- The old configuration properties ("*oraclebmc*") are deprecated; please use ("*oci*") instead. The old properties still work for backward compatibility, as long as the corresponding new property isn't set at the same time.
- The old HDFS scheme ("oraclebmc") is deprecated; please use "oci" instead. The old scheme still works for backward compatibility.

## 1.2.3 - 2017-04-06
### Fixed
- Updated to latest Java SDK (1.2.5) to pick up change for request id truncation (to fix multipart uploads)

### Changed
- Changed properties and constants to allow for more useful documentation
- Updated maven shade plugin to non-snapshot version

## 1.2.2 - 2017-03-28
### Changed
- Internal changes for how properties are loaded

### Added
- Support to use multi-part uploads when saving files
- Configuration options to tune multi-part upload behavior (or disable it)

## 1.2.1 - 2017-03-16
### Fixed
- Bug in directory listing resulting in duplicate directories
- Concurrency issue when creating directory placeholders

### Changed
- Improved "list directory" performance for large directories

## 1.2.0 - 2016-12-16
### Fixed
- Using correct Date header for object creation time
- Bug with seek operation

### Changed
- Updated to Oracle Cloud Infrastructure Java SDK 1.2.0
- Shading a few more dependencies (h2k)
- Doc updates

### Added
- Abstract Filesystem to support usagage within Yarn and Spark

## 1.1.0 - 2016-11-18
### Fixed
- Updated to Oracle BMCS Java SDK 1.1.0 to pick up bug fixes

### Added
- License/copyright headers added to all source files as part of the build
- Now relocating shaded packages for Bouncycastle, Apache Commons, Glassfish

## 1.0.1 - 2016-11-15
### Fixed
- Updated to Oracle Cloud Infrastructure Java SDK 1.0.1 to pick up bug fixes

### Added
- Including MD5 validation during copy operations

## 1.0.0 - 2016-10-20
### Added
- Initial Release
- Support added for Hadoop 2.7.2 using Oracle Cloud Infrastructure Services Java SDK 1.0.0
