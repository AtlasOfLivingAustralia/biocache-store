#!/bin/sh

mvn clean install -DskipTests=true -DinitialMemorySize=8g -DmaximumMemorySize=8g
cp target/biocache-store-*-distribution.zip ./
