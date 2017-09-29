#!/bin/sh

mvn clean install -DskipTests=true -DinitialMemorySize=8g -DmaxMemorySize=8g
cp target/biocache-store-*-distribution.zip ./
