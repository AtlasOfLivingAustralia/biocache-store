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
or wherever the ```name.dir``` is configured to point to in your ``` /data/biocache/config/biocache-config.properties ``` file.

To skip the tests during the build, run ```mvn install -DskipTests=true```.

## Releases

Latest release is 1.6.3. For more information [click here](https://github.com/AtlasOfLivingAustralia/biocache-store/releases)
