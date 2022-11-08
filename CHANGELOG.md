# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).

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
