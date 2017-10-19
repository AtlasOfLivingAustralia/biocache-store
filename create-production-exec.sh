#!/bin/sh

mvn clean install -DskipTests=true -DinitialMemorySize=16g -DmaxMemorySize=16g
cp target/biocache-store-*-distribution.zip ./
