#!/usr/bin/env bash

scp target/biocache-store* c0.nbnatlas.org:/tmp/ &
scp target/biocache-store* c1.nbnatlas.org:/tmp/ &
scp target/biocache-store* c2.nbnatlas.org:/tmp/ &
scp target/biocache-store* c3.nbnatlas.org:/tmp/ &
