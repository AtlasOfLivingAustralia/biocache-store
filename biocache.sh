#!/bin/sh
exec java -Djava.util.Arrays.useLegacyMergeSort=true -Xmx16g -Xms16g  -jar $0 "$@"


