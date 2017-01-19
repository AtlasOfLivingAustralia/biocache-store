#!/bin/sh

mvn clean install -DskipTests=true
cp target/biocache-store-*-distribution.zip ./
