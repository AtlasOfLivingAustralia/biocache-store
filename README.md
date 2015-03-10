# biocache-store  [![Build Status](https://travis-ci.org/AtlasOfLivingAustralia/biocache-store.svg?branch=master)](http://travis-ci.org/AtlasOfLivingAustralia/biocache-store)

Scala implementation of biocache backend.
This code base manages the loading, sampling, processing and indexing of occurrence records for the ALA.
There are additional tools to support outlier detection, duplicate detection and identifying extra-limital outliers
based on authoritative distribution polygons for taxa.
Please use the Ansible scripts to set this software up in your own environment.

https://github.com/AtlasOfLivingAustralia/ala-install

## Build notes

This library is built with maven. By default a `mvn install` will try to run a test suite which will fail without a local installation of a name index.
The name index can be downloaded [here](http://biocache.ala.org.au/archives/nameindexes/20140610/namematching.tgz) and needs to be extracted to the
directory `/data/lucene/namematching`

To skip this step, run a build with ```mvn install -DskipTests=true```.

or wherever the ```name.dir``` is configured to point to in your ``` /data/biocache/config/biocache-config.properties ``` file.

## Release notes - version 1.3

* additional support for retrieve geotags for flickr
* conditional log warning for sampling
* only log warning if there are a set of fields to sample
* use country code to resolve country where nothing else available
* reduced memory - no longer sampling locally
* loader exception handling
* now using ConcurrentUpdateSolrServer for updates to SOLR
* load all resources option
* bulk loading
* Added ala parent pom reference
* Removed unnecessary repository references inside pom
* Added exclusions to gitignore file
* groovy script for cleaning up directories
* blacklisted URLs
* additional options for column export - specify column separator and row
* bug fix for empty rows - this was causing the /occurrences/deleted
* service to fail with ArrayOutOfBoundsExceptions
* Add sampling progress feedback
* another fix for remote sampling
* sampling fix and cleanup
* check for non-numeric value
* remote sampling fix
* version info in built jar - now compatible with travis builds
* updated maven repository credentials
* distributionManagement
* skip tests for now
* isLoadable
* sensitive test fixes - cat1 tests added but commented out
* abort if row key file not there
* removed dynamic layer lookup
* using a different plugin to generate git information
* additional date format support
* species subgroup config URL
* service exposing species subgroup config URL
* export util includeRowKey = false
* use the supplied number of threads
* added an additional repo in attempt to fix travis build
* fix for biocache load dir paths
* Occurrence status and missing description
* bumped minor scala version
* code cleanup
* occurrence status processing
* processing of occurrence status
* formatting
* code formatting
* externally configurable species groups
* not a type
* refactoring of media downloads
* added missing flush which was causing the export to be truncated
* Merge remote-tracking branch 'origin/master'
* added travis build status
* Prevent DigiVol ingestion from creating a data set if the guid is null
* added a travis profile to skip the git-commit step which is problematic for travis
* Prevent DigiVol ingestion from creating data sets if the guid (project home page url) is empty or null
* unique set of load, processed and QA dates
* cmd tool to retrieve human readable timestamps for fields for debugging
* load currency
* removed gzip zip which was broken
* reverted git-commit version bump - verifyerrors
* code cleanup, formatting and logging fixes, removal of redundant code
* cleanup of boolean cmd args
* delete by file of UUIDs
* problematic when data has not been provided in utf8
* cleanup of boolean cmd args so that there isn’t a need to specify a
* removed redundant plugin
* bumped up the version of the maven git-commit plugin
* changes to keep up with BVP web service changes
* fixes for downloading media
* Comma separated values in associatedMedia
* Support for comma separated values in associatedMedia needed for
* iNaturalist
* Minor readme changes
* javadoccing and code style in an attempt to make the code more readable
* formatting and typos
* removed unused imports
* Range deletor for deleting records by a start and end key range
* code readability
* javadocc-ing of duplicate detection and code cleanup - more to be done
* code formatting and cleanup
* code formatting to comply with code standards and cleanup
* support export by key range
* support CSV export by key range
* order config properties
* order config properties lexicographically in “biocache config” command
* reverted changes of bad commit
* dont include the rowkey in the DWCA export
* Readme fixes for name index information
* data loader handling of empty unique terms
* data loader handling of data resources with an empty list of unique
* markdown for code



## Release notes - version 1.2

 * reindexing fixes
 * fixes to test
 * lucene indexes
 * changes to allow compiling with java 6
 * distribution package changes
 * this will be manipulated by ansible scripts to create an executable jar
 * Git version information for command line tools
 * added the ability to override vocabulary files with files external to built executables
 * change SCM config to Git
 * removal of println, and obsolete imports
 * fixes to make media loading more robust, and changes to duplicate detection to avoid JSON serialisation problems
 * full file extraction with support for Zips with subdirectories (required for GBIF archive loading)
 * bvp hub update of data resources
 * removed old sandbox code to reduce confusion
 * jackson update
 * jacknife fix
 * BVP data harvesting support
 * typo in log statement
 * case insensitive check
 * fixes for sounds files
 * remote media store support
 * resampling support
 * split index merge into separate tool
 * enhancements for a more executable biocache package - still requiring a maven job to package final executable

## Release notes - version 1.1

 * changes for release management
 * Copy new package structure
 * committed after review
 * Fix for Embedded Solr Server
 * build fix
 * added patches from r4273
 * removed println from tests
 * scaladoc
 * remove println
 * doco and minor fixes for consistency
 * fixes for package path
 * reorganisation of packages
