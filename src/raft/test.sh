#!/bin/bash

echo "Begin "$1
for i in {1..10}
do
echo 'test round '$i': '
GO111MODULE=off go test -run $1
done
