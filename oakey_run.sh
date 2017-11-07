#!/bin/bash

set -eu

REMOTE="oakey.cs.umass.edu"

ssh $REMOTE "cd code/supervised_prf && java -Xmx8G -ea -cp target/remote.jar $@"
