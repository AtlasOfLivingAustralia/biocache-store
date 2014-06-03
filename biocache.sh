#!/bin/sh
exec java -Dfile.encoding=UTF8 -Dactors.corePoolSize=8 -Dactors.maxPoolSize=16 -Dactors.minPoolSize=8 -Djava.util.Arrays.useLegacyMergeSort=true -Xmx16g -Xms16g  -jar $0 "$@"


