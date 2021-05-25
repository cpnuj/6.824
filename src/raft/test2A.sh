#!/bin/zsh

for i in {1..20}
do
echo 'test round '$i' fails: '
go test -run 2A | grep 'FAIL' | wc -l
done

