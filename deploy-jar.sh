#!/usr/bin/env bash

scp target/biocache-store-2.0-SNAPSHOT.jar c0.nbnatlas.org:/tmp/ &
scp target/biocache-store-2.0-SNAPSHOT.jar c1.nbnatlas.org:/tmp/ &
scp target/biocache-store-2.0-SNAPSHOT.jar c2.nbnatlas.org:/tmp/ &
scp target/biocache-store-2.0-SNAPSHOT.jar c3.nbnatlas.org:/tmp/ &