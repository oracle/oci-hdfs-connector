# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).

## Unreleased
### Fixed

### Changed

### Added

## 1.2.9 - 2018-02-22
### Changed
- Release to [GitHub](https://github.com/oracle/oci-hdfs-connector)

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
- Updated to Oracle BMCS Java SDK 1.2.0
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
- Updated to Oracle BMCS Java SDK 1.0.1 to pick up bug fixes

### Added
- Including MD5 validation during copy operations

## 1.0.0 - 2016-10-20
### Added
- Initial Release
- Support added for Hadoop 2.7.2 using Oracle Bare Metal Cloud Services Java SDK 1.0.0
