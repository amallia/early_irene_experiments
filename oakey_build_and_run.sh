#!/bin/bash

set -eu

REMOTE="oakey.cs.umass.edu"

mvn install
scp target/sprf-0.1-SNAPSHOT.jar $REMOTE:code/supervised_prf/target/remote.jar
ssh $REMOTE "cd code/supervised_prf && java -Xmx8G -ea -cp target/remote.jar $@"
