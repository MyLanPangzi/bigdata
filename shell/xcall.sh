#!/usr/bin/env bash

for i in hadoop102 hadoop103 hadoop104 ; do
   echo "$i"
    ssh -t $i "$@"
done

