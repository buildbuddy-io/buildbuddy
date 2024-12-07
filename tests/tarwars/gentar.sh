#!/bin/bash
set -o errexit -o nounset -o pipefail

FILES=8000
MAXDEPTH=5
MAXWIDTH=10

# create a directory next to this script
cd "$(dirname "$0")"

TEMPDIR=$(mktemp -d)

for i in `seq $FILES`; do
    DIR="$TEMPDIR"
    DEPTH=$(( (RANDOM % $MAXDEPTH) + 1 ))
    for d in `seq $DEPTH`; do
	W=$(( (RANDOM % $MAXWIDTH) + 1 ))
	DIR=${DIR%%+(/)}${DIR:+/}$W
    done
    mkdir -p $DIR
    echo $i > $DIR/$i.txt;
done

tar czf bigbad.tar.gz $TEMPDIR
rm -rf $TEMPDIR
