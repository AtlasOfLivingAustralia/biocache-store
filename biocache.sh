#!/bin/sh
exec java $BIOCACHE_OPTS -Dfile.encoding=UTF8 -Dactors.corePoolSize=8 -Dactors.maxPoolSize=16 -Dactors.minPoolSize=8 -Djava.util.Arrays.useLegacyMergeSort=true -Xmx8g -Xms8g  -jar $0 "$@"


