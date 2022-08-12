#!/bin/bash

echo "Begin "$1
for i in {1..1}
do
echo 'test round '$i': '
go test -run $1
done
