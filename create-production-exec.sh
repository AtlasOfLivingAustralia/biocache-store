#!/bin/sh

mvn clean install -DskipTests=true -DinitialMemorySize=12g -DmaximumMemorySize=12g
cp target/biocache-store-*-distribution.zip ./
