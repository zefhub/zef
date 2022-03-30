#!/bin/zsh

export ZEFDB_LOGIN_AUTOCONNECT=FALSE

tag=${1:-UNTAGGED}

echo "# num_entities num_batches time_in_ms" >> bmarks.dat
for iter in $(seq 10) ; do
    for n in 10 100 1000 ; do
        for m in 1 10 100 ; do
            time=$(src_cpp/c_tests/c_test $n $m 2>&1 | grep "Time was" | awk '{print $3}' && exit ${pipestatus[1]})
            if [[ $? != 0 ]] ; then
               echo "The c_test failed!"
               exit 1
            fi

            echo $n $m $tag $time >> bmarks.dat
            echo $n $m $tag $time
        done
    done
done