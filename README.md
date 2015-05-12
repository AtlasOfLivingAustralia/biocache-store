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

## Release notes - version 1.4
- [92b8a9d](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/92b8a9dd917c6976717b65082b6fff3bc2523c27) ingest skip stages options (@djtfmartin)
- [4a12610](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/4a1261072ebd986c2201227e2ca7bb9916cf4e4c) doco for cmd tool (@djtfmartin)
- [9c24e7c](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/9c24e7c0ed86ac597cc68aabad2ca579ff3d903c) log indexing errors and continue (@djtfmartin)
- [afb2612](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/afb2612bb02df0a036316841d25db74637a50044) gbif dwca fix (@djtfmartin)
- [127b203](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/127b2033ea6568ebee58f9592b20b298190f3daf) CSV export on index sensitive (@adam-collins)
- [9f1df9e](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/9f1df9e8acc3cdeddf7627ac64c792661d96f297) Pipeline delimited recordedBy values fixed. (@sadeghim)
- [df9cfac](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/df9cfacc622aba82aa4ce624025bb9e4aeef3268) set the occurrenceID (@djtfmartin)
- [bcde105](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/bcde1055ce46538cbd11d581bf19973bc7f97510) recognise image service URLs and act appropriately (@djtfmartin)
- [4d94298](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/4d94298e0ad8aa09421c85d3ad103bc77042cfc3) catch JSON parse errors with outlierForLayers.p (@adam-collins)
- [d022861](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/d0228617fe08550e66c9fe647f3304a95980a90e) CSV export on index sensitive (@adam-collins)
- [a6dc107](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/a6dc1076e0dbad97bd769d91698bedd83d8b5da7) CSV export on index missing newline (@adam-collins)
- [98d2e11](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/98d2e11beb5482ca3c63844ada6c8b2493da7c0b) Separate CSV export on index for sensitive (@adam-collins)
- [4986928](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/49869282d7185c59f02f43b22b3b56a96366d972) Fix for missing parameter additional.fields.to.index (@adam-collins)
- [9665417](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/9665417c6ac5a5d65497b4c4fb2eadf8ffecb400) Add index on point-0.02 (@adam-collins)
- [f95c4a1](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/f95c4a13d3eda6f068611992d062a84df3af6beb) The method readAsCSV was not looping over the rows. (@sadeghim)
- [1e9f74b](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/1e9f74b1d9ac9286c8a5f4b9635b81a70f722cb5) Was returning no result ora result without any columns. First parameter is the starting column name which should have been passed as "rowKey". EMPTY works for now. (@sadeghim)
- [222692b](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/222692b2bab6d18b2eb1c8ffb29f3d2fae0189c3) Update layer defaults. (@adam-collins)
- [5d89d87](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/5d89d871e25799666bbe3aa8191e19700f511b2c) remove caching of sampling fields (@adam-collins)
- [bba4766](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/bba47669623b3d9322088ae52df93967798f3763) ignore target directory (@djtfmartin)
- [8ecc255](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/8ecc255f454bf164d1c65beb10254bd8e5d0aba4) additional field mapping (@djtfmartin)
- [6806934](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/6806934b10a84290053347497e3ce2da0a8489fa) support for pipe delimited associatedMedia string (@djtfmartin)
- [1cafa26](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/1cafa26817f98bd8b43a662344bed7d95b3ac29c) code formatting (@djtfmartin)
- [762de37](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/762de3723583d4636123097409645ac809192269) removed addRaw (@djtfmartin)
- [18c6450](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/18c64500b0f13bc07135f693b3439e2f382dedf6) MD5 hashing of image file names (@djtfmartin)
- [8f3b331](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/8f3b3311cceb08ca9ee2ca6a358efad49f050b73) EOL fixes (@djtfmartin)
- [6a30d1f](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/6a30d1f39c24d1efc3002ca761db4fe719eca779) prototype loader of EOL images (@djtfmartin)
- [c5608a9](https://github.com/AtlasOfLivingAustralia/biocache-store/commit/c5608a985a210de176d94358f561978349078933) exception handling in date parsing (@djtfmartin)


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
