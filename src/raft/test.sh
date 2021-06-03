#!/bin/zsh

echo "Begin "$1
for i in {1..20}
do
echo 'test round '$i' fails: '
go test -run $1 | grep 'FAIL' | wc -l
done

