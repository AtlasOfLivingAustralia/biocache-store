# biocache-store

Scala implementation of biocache backend.
This code base manages the loading, processing, sampling and indexing of occurrence records for the ALA.
There are additional tools to support outlier detection, duplicate detection and identifying extra-limital outliers
based on authoritative distribution polygons for taxa.
Please use the Ansible scripts to set this software up in your own environment.

https://github.com/gbif/ala-install

Questions and comment on the scripts are welcome here: ala-portal@lists.gbif.org

## Build notes

Currently a build with ```mvn install``` requires a local lucene names index for the tests to pass.
To get around this, run with ```mvn install -DskipTests=true```.
A lucene names index can be accessed here:

http://downloads.ala.org.au/

and should be un-packaged to:

```/data/lucene/```

or wherever the ```name.dir``` is configured to point to in your ```/data/biocache/config/biocache-config.properties``` file.

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