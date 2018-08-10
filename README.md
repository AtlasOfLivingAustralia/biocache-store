# biocache-store  [![Build Status](https://travis-ci.org/AtlasOfLivingAustralia/biocache-store.svg?branch=master)](http://travis-ci.org/AtlasOfLivingAustralia/biocache-store)

Scala implementation of biocache backend.
This code base manages the loading, sampling, processing and indexing of occurrence records for the ALA.
There are additional tools to support outlier detection, duplicate detection and identifying extra-limital outliers
based on authoritative distribution polygons for taxa.
Please use the Ansible scripts to set this software up in your own environment.

https://github.com/AtlasOfLivingAustralia/ala-install

## Releases

We are currently running 2 development branches

* master branch is using cassandra 3.11 and SOLR Cloud 6 or 7.
* legacy 1.9 which is using cassandra 1.x, with SOLR 4. 

## Build notes

This library is built with maven. By default a `mvn install` will try to run a test suite which will fail without a local installation of a name index.
The name index can be downloaded [here](http://biocache.ala.org.au/archives/nameindexes/20160229/namematching.tgz) and needs to be extracted to the
directory `/data/lucene/namematching`
or wherever the ```name.dir``` is configured to point to in your ``` /data/biocache/config/biocache-config.properties ``` file.

Also, to pass SDS tests the system requires sds layers. They can be downloaded [here](http://biocache.ala.org.au/archives/layers/sds-layers.tgz). Or, if that link does not work try [here](http://biocache-imt.ala.org.au/archives/layers/sds-layers.tgz). Extract the content of this file into `/data/biocache/layers`.

To skip the tests during the build, run ```mvn install -DskipTests=true```.

## Releases

For a list of releases [click here](https://github.com/AtlasOfLivingAustralia/biocache-store/releases)
For a list of built releases, see [here](http://nexus.ala.org.au/#nexus-search;quick~biocache-store)

## Version 2.x

The 2.x version of this code base  that makes use of cassandra 3.x and SOLR 6.x with SOLR cloud support.
See the [cassandra3](https://github.com/AtlasOfLivingAustralia/biocache-store/tree/cassandra3) branch for details.

There are differences when using cassandra.case.sensitive=true
- case sensitive cassandra schema
- no remapping of class to classs and order to bioorder

solr.collection config var is mandatory

## Acknowledgements
YourKit is kindly supporting open source projects with its full-featured Java Profiler. YourKit, LLC is the creator of innovative and intelligent tools for profiling Java and .NET applications.

[![Yourkit](https://www.yourkit.com/images/yklogo.png)](http://www.yourkit.com)

Take a look at YourKit's leading software products: <a href="http://www.yourkit.com/java/profiler/index.jsp">YourKit Java Profiler</a> and <a href="http://www.yourkit.com/.net/profiler/index.jsp">YourKit .NET Profiler</a>.

JetBrains is also a big supporter of open source projects and has kindly provided licenses for their fantastic IDE IntelliJ to ALA. Learn more at <a href="http://www.jetbrains.com/idea/">the IntelliJ site</a>.
