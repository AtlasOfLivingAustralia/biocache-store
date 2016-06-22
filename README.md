# biocache-store  [![Build Status](https://travis-ci.org/AtlasOfLivingAustralia/biocache-store.svg?branch=master)](http://travis-ci.org/AtlasOfLivingAustralia/biocache-store)

Scala implementation of biocache backend.
This code base manages the loading, sampling, processing and indexing of occurrence records for the ALA.
There are additional tools to support outlier detection, duplicate detection and identifying extra-limital outliers
based on authoritative distribution polygons for taxa.
Please use the Ansible scripts to set this software up in your own environment.

https://github.com/AtlasOfLivingAustralia/ala-install

## Build notes

This library is built with maven. By default a `mvn install` will try to run a test suite which will fail without a local installation of a name index.
The name index can be downloaded [here](http://biocache.ala.org.au/archives/nameindexes/20160229/namematching.tgz) and needs to be extracted to the
directory `/data/lucene/namematching`
or wherever the ```name.dir``` is configured to point to in your ``` /data/biocache/config/biocache-config.properties ``` file.

To skip the tests during the build, run ```mvn install -DskipTests=true```.

## Releases

For a list of releases [click here](https://github.com/AtlasOfLivingAustralia/biocache-store/releases)
For a list of built releases, see [here](http://nexus.ala.org.au/#nexus-search;quick~biocache-store)


## Acknowledgements
YourKit is kindly supporting open source projects with its full-featured Java Profiler. YourKit, LLC is the creator of innovative and intelligent tools for profiling Java and .NET applications.

[![Yourkit](https://www.yourkit.com/images/yklogo.png)](http://www.yourkit.com)

Take a look at YourKit's leading software products: <a href="http://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a> and <a href="http://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>.

JetBrains is also a big supporter of open source projects and has kindly provided licenses for their fantastic IDE IntelliJ to ALA. Learn more at <a href="http://www.jetbrains.com/idea/">the IntelliJ site</a>.
